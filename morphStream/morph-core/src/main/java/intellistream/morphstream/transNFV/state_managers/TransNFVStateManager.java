package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.adaptation.PerformanceModel;
import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.transNFV.adaptation.IterativeWorkloadMonitor;
import intellistream.morphstream.transNFV.common.PatternData;
import intellistream.morphstream.transNFV.executors.OffloadExecutor;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.jpmml.evaluator.ModelEvaluator;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TransNFVStateManager {
    private IterativeWorkloadMonitor workloadMonitor;
    private Thread WorkloadMonitorThread;

    private PartitionStateManager partitionStateManager;
    private Thread partitionStateManagerThread;
    private ReplicationStateManager replicationStateManager;
    private Thread replicationStateManagerThread;
    private final HashMap<Integer, Thread> offloadExecutorThreads = new HashMap<>();
    private final HashMap<Integer, OffloadExecutor> offloadExecutors = new HashMap<>();
    private final OffloadSVCCStateManager svccStateManager = new OffloadSVCCStateManager();
    private final OffloadMVCCStateManager mvccStateManager = new OffloadMVCCStateManager();

    private OpenNFStateManager openNFStateManager;
    private Thread openNFThread;
    private CHCStateManager chcStateManager;
    private Thread chcThread;
    private S6StateManager s6StateManager;
    private Thread s6Thread;

    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> partitioningQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> replicationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> openNFQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> chcQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> s6Queue = new LinkedBlockingQueue<>();
    public final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadInputQueues = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final boolean hardcodeSwitch = (MorphStreamEnv.get().configuration().getInt("hardcodeSwitch") == 1);


    private ModelEvaluator<?> modelEvaluator;


    public TransNFVStateManager() {
        int partitionGap = tableSize / numInstances;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap); //TODO: Refine this
        }
        String pmmlFilePath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/mlp_model.pmml";
        modelEvaluator = PerformanceModel.loadPMMLModel(pmmlFilePath);
    }

    public void prepareWorkloadMonitor() {
        workloadMonitor = new IterativeWorkloadMonitor(monitorQueue);
        WorkloadMonitorThread = new Thread(workloadMonitor);
    }

    public void preparePartitionStateManager() {
        partitionStateManager = new PartitionStateManager(partitioningQueue);
        partitionStateManagerThread = new Thread(partitionStateManager);
    }

    public void prepareReplicationStateManager() {
        replicationStateManager = new ReplicationStateManager(replicationQueue);
        replicationStateManagerThread = new Thread(replicationStateManager);
    }

    public void prepareOffloadExecutors() {
        for (int i = 0; i < numOffloadThreads; i++) {
            BlockingQueue<VNFRequest> inputQueue = new LinkedBlockingQueue<>();
            offloadInputQueues.put(i, inputQueue);
            OffloadExecutor offloadExecutor = new OffloadExecutor(i, inputQueue, svccStateManager, mvccStateManager);
            offloadExecutors.put(i, offloadExecutor);
            Thread offloadExecutorThread = new Thread(offloadExecutor);
            offloadExecutorThreads.put(i, offloadExecutorThread);
        }
    }

    public void prepareProactiveExecutors() {
        int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread"); //Number of thread for TPG_CC
        for (int i = 0; i < tpgThreadNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgInputQueues.put(i, inputQueue);
        }
    }

    public void prepareOpenNFStateManager() {
        openNFStateManager = new OpenNFStateManager(openNFQueue);
        openNFThread = new Thread(openNFStateManager);
    }

    public void prepareS6StateManager() {
        s6StateManager = new S6StateManager(s6Queue);
        s6Thread = new Thread(s6StateManager);
    }

    public void prepareCHCStateManager() {
        chcStateManager = new CHCStateManager(chcQueue);
        chcThread = new Thread(chcStateManager);
    }

    public void prepareAdaptiveCC() {
        //TODO: Currently we only prepare the partitioning and offloading managers.
        partitionStateManager = new PartitionStateManager(partitioningQueue);
        partitionStateManagerThread = new Thread(partitionStateManager);

        for (int i = 0; i < numOffloadThreads; i++) {
            BlockingQueue<VNFRequest> inputQueue = new LinkedBlockingQueue<>();
            offloadInputQueues.put(i, inputQueue);
            OffloadExecutor offloadExecutor = new OffloadExecutor(i, inputQueue, svccStateManager, mvccStateManager);
            offloadExecutors.put(i, offloadExecutor);
            Thread offloadExecutorThread = new Thread(offloadExecutor);
            offloadExecutorThreads.put(i, offloadExecutorThread);
        }
    }

    public void startAdaptiveCC() {
        partitionStateManagerThread.start();
        System.out.println("Partitioning controller started");

        for (int i = 0; i < numOffloadThreads; i++) {
            offloadExecutorThreads.get(i).start();
        }
        System.out.println("Offload executors started");

        if (!hardcodeSwitch) {
            WorkloadMonitorThread.start();
            System.out.println("Batch workload monitor started");
        }
    }

    public void joinAdaptiveCC() {
        try {
            partitionStateManagerThread.join();
            for (int i = 0; i < numOffloadThreads; i++) {
                offloadExecutorThreads.get(i).join();
            }
            if (!hardcodeSwitch) {
                WorkloadMonitorThread.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startPartitionStateManager() {
        partitionStateManagerThread.start();
        System.out.println("Partitioning controller started");
    }

    public void joinPartitionStateManager() {
        try {
            partitionStateManagerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startReplicationStateManager() {
        replicationStateManagerThread.start();
        System.out.println("Cache controller started");
    }

    public void joinReplicationStateManager() {
        try {
            replicationStateManagerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startOffloadExecutors() {
        for (int i = 0; i < numOffloadThreads; i++) {
            offloadExecutorThreads.get(i).start();
        }
        System.out.println("Offload executors started");
    }

    public void joinOffloadExecutors() {
        try {
            for (int i = 0; i < numOffloadThreads; i++) {
                offloadExecutorThreads.get(i).join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startOpenNFStateManager() {
        openNFThread.start();
    }
    public void joinOpenNFStateManager() {
        try {
            openNFThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void startCHCStateManager() {
        chcThread.start();
    }
    public void joinCHCStateManager() {
        try {
            chcThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void startS6StateManager() {
        s6Thread.start();
    }
    public void joinS6StateManager() {
        try {
            s6Thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public PartitionStateManager getPartitionStateManager() {
        return partitionStateManager;
    }

    public ReplicationStateManager getReplicationStateManager() {
        return replicationStateManager;
    }

    public HashMap<Integer, OffloadExecutor> getOffloadExecutors() {
        return offloadExecutors;
    }

    public long getOffloadAvgAggUsefulTime() {
        long aggUsefulTime = 0;
        for (int i = 0; i < numOffloadThreads; i++) {
            aggUsefulTime += offloadExecutors.get(i).getAGG_USEFUL_TIME();
        }
        return aggUsefulTime / numOffloadThreads;
    }

    public long getOffloadAvgAggParsingTime() {
        long aggParsingTime = 0;
        for (int i = 0; i < numOffloadThreads; i++) {
            aggParsingTime += offloadExecutors.get(i).getAGG_PARSING_TIME();
        }
        return aggParsingTime / numOffloadThreads;
    }

    public OpenNFStateManager getOpenNFStateManager() {
        return openNFStateManager;
    }

    public CHCStateManager getCHCStateManager() {
        return chcStateManager;
    }

    public S6StateManager getS6StateManager() {
        return s6StateManager;
    }

    public IterativeWorkloadMonitor getWorkloadMonitor() {
        return workloadMonitor;
    }

    public BlockingQueue<TransactionalEvent> getTPGInputQueue(int spoutId) {
        return tpgInputQueues.get(spoutId);
    }

    public BlockingQueue<VNFRequest> getOffloadingInputQueue(int offloadingId) {
        return offloadInputQueues.get(offloadingId);
    }

    public ModelEvaluator<?> getModelEvaluator() {
        return modelEvaluator;
    }
}
