package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.transNFV.adaptation.BatchMonitorThread;
import intellistream.morphstream.transNFV.common.PatternData;
import intellistream.morphstream.transNFV.executors.OffloadExecutorThread;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class UniversalStateManager {
    private Thread monitorThread;
    private Thread partitioningCCThread;
    private Thread replicationCCThread;
    private HashMap<Integer, Thread> offloadThreads = new HashMap<>();
    private static final HashMap<Integer, OffloadExecutorThread> offloadExecutorThreads = new HashMap<>();
    private static final OffloadSVCCStateManager svccStateManager = new OffloadSVCCStateManager();
    private static final OffloadMVCCStateManager mvccStateManager = new OffloadMVCCStateManager();
    private Thread openNFThread;
    private Thread chcThread;
    private Thread s6Thread;
    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> partitioningQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> replicationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> openNFQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> chcQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> s6Queue = new LinkedBlockingQueue<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadInputQueues = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgInputQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final int numOffloadThreads = MorphStreamEnv.get().configuration().getInt("numOffloadThreads");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");


    public UniversalStateManager() {
        monitorThread = new Thread(new BatchMonitorThread(monitorQueue));
        partitioningCCThread = new Thread(new PartitionStateManager(partitioningQueue));
        replicationCCThread = new Thread(new ReplicationStateManager(replicationQueue));
        openNFThread = new Thread(new OpenNFStateManager(openNFQueue));
        chcThread = new Thread(new CHCStateManager(chcQueue));
        s6Thread = new Thread(new S6StateManager(s6Queue));

        int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread"); //Number of thread for TPG_CC
        for (int i = 0; i < tpgThreadNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgInputQueues.put(i, inputQueue);
        }

        for (int i = 0; i < numOffloadThreads; i++) {
            BlockingQueue<VNFRequest> inputQueue = new LinkedBlockingQueue<>();
            offloadInputQueues.put(i, inputQueue);
            OffloadExecutorThread offloadExecutorThread = new OffloadExecutorThread(i, inputQueue, svccStateManager, mvccStateManager);
            offloadExecutorThreads.put(i, offloadExecutorThread);
            Thread offloadThread = new Thread(offloadExecutorThread);
            offloadThreads.put(i, offloadThread);
        }

        int partitionGap = tableSize / numInstances;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap); //TODO: Refine this
        }
    }

    public void startAdaptiveCC() {
        throw new UnsupportedOperationException("Adaptive state management to be updated");
//        monitorThread.start();
//        replicationCCThread.start();
//        for (int i = 0; i < numOffloadThreads; i++) {
//            offloadThreads.get(i).start();
//        }
//        System.out.println("CC123 and Monitor started");
    }

    public void joinAdaptiveCC() {
        throw new UnsupportedOperationException("Adaptive state management to be updated");
//        try {
//            monitorThread.join();
//            replicationCCThread.join();
//            for (int i = 0; i < numOffloadThreads; i++) {
//                offloadThreads.get(i).join();
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void startPartitioningCC() {
        partitioningCCThread.start();
        System.out.println("Partitioning controller started");
    }

    public void joinPartitioningCC() {
        try {
            partitioningCCThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startReplicationCC() {
        replicationCCThread.start();
        System.out.println("Cache controller started");
    }

    public void joinReplicationCC() {
        try {
            replicationCCThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startOffloadExecutorThreads() {
        for (int i = 0; i < numOffloadThreads; i++) {
            offloadThreads.get(i).start();
        }
        System.out.println("Offload executors started");
    }

    public void joinOffloadExecutorThreads() {
        try {
            for (int i = 0; i < numOffloadThreads; i++) {
                offloadThreads.get(i).join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startOpenNF() {
        openNFThread.start();
    }
    public void joinOpenNF() {
        try {
            openNFThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void startCHC() {
        chcThread.start();
    }
    public void joinCHC() {
        try {
            chcThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void startS6() {
        s6Thread.start();
    }
    public void joinS6() {
        try {
            s6Thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public BlockingQueue<TransactionalEvent> getTPGInputQueue(int spoutId) {
        return tpgInputQueues.get(spoutId);
    }

    public BlockingQueue<VNFRequest> getOffloadingInputQueue(int offloadingId) {
        return offloadInputQueues.get(offloadingId);
    }

    public static HashMap<Integer, OffloadExecutorThread> getOffloadExecutorThreads() {
        return offloadExecutorThreads;
    }

}
