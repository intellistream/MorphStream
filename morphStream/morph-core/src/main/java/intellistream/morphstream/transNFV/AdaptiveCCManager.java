package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.transNFV.data.PatternData;
import intellistream.morphstream.transNFV.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import message.VNFCtlStub;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveCCManager {
    private Thread monitorThread;
    private Thread partitionCCThread;
    private Thread replicationCCThread;
    private Thread offloadCCExecutorService;
    private Thread offloadStateManagerThread;
    private HashMap<Integer, Thread> offloadExecutorThreads = new HashMap<>();
    private Thread openNFThread;
    private Thread chcThread;
    private Thread s6Thread;
    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> partitionQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> replicationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> offloadQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> openNFQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> chcQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> s6Queue = new LinkedBlockingQueue<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    public static HashMap<Integer, VNFCtlStub> vnfStubs = new HashMap<>();
    private VNFRunner vnfManager;
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private final int offloadCCThreadNum = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private final int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static HashMap<Integer, Long> tpgThreadInitEndTimes = new HashMap<>(); // bolt thread id -> end time of bolt initialization
    private static HashMap<Integer, Long> tpgThreadProcessEndTimes = new HashMap<>();

    public AdaptiveCCManager() {
        monitorThread = new Thread(new BatchMonitorThread(monitorQueue));
        partitionCCThread = new Thread(new PartitionCCThread(partitionQueue, partitionOwnership));
        replicationCCThread = new Thread(new ReplicationCCThread(replicationQueue));
        offloadCCExecutorService = new Thread(new OffloadCCExecutorService(offloadQueue, offloadCCThreadNum));
        offloadStateManagerThread = new Thread(new OffloadStateManager());
        openNFThread = new Thread(new OpenNFController(openNFQueue));
        chcThread = new Thread(new CHCController(chcQueue));
        s6Thread = new Thread(new S6Controller(s6Queue));

        int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread"); //Number of thread for TPG_CC
        for (int i = 0; i < tpgThreadNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgQueues.put(i, inputQueue);
        }

        for (int i = 0; i < offloadCCThreadNum; i++) {
            BlockingQueue<VNFRequest> inputQueue = new LinkedBlockingQueue<>();
            offloadingQueues.put(i, inputQueue);
        }
        for (int i = 0; i < offloadCCThreadNum; i++) {
            Thread offloadExecutorThread = new Thread(new OffloadExecutor(i));
            offloadExecutorThreads.put(i, offloadExecutorThread);
        }

        int partitionGap = tableSize / vnfInstanceNum;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap); //TODO: Refine this
        }
    }

    public void startAdaptiveCC() {
        monitorThread.start();
        partitionCCThread.start();
        replicationCCThread.start();
        offloadCCExecutorService.start();
        System.out.println("CC123 and Monitor started");
    }

    public void joinAdaptiveCC() {
        try {
            monitorThread.join();
            partitionCCThread.join();
            replicationCCThread.join();
            offloadCCExecutorService.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startPartitionCC() {
        partitionCCThread.start();
        System.out.println("Partition controller started");
    }

    public void joinPartitionCC() {
        try {
            partitionCCThread.join();
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

    public void startOffloadCCExecutorService() {
        offloadCCExecutorService.start();
        System.out.println("Offload executor service started");
    }

    public void joinOffloadCCExecutorService() {
        try {
            offloadCCExecutorService.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startOffloadExecutorThreads() {
        offloadStateManagerThread.start();
        for (int i = 0; i < offloadCCThreadNum; i++) {
            offloadExecutorThreads.get(i).start();
        }
        System.out.println("Offload executors started");
    }

    public void joinOffloadExecutorThreads() {
        try {
            offloadStateManagerThread.join();
            for (int i = 0; i < offloadCCThreadNum; i++) {
                offloadExecutorThreads.get(i).join();
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
        return tpgQueues.get(spoutId);
    }

    public BlockingQueue<VNFRequest> getOffloadingInputQueue(int offloadingId) {
        return offloadingQueues.get(offloadingId);
    }

    public String getPattern() {
        if (pattern == 0) {
            return "loneOperative";
        } else if (pattern == 1) {
            return "sharedReaders";
        } else if (pattern == 2) {
            return "sharedWriters";
        } else if (pattern == 3) {
            return "mutualInteractive";
        } else if (pattern == 4) {
            return "dynamic";
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public String getCCStrategy() {
        if (ccStrategy == 0) {
            return "Partitioning";
        } else if (ccStrategy == 1) {
            return "Replication";
        } else if (ccStrategy == 2) {
            return "Offloading";
        } else if (ccStrategy == 3) {
            return "Preemptive";
        } else if (ccStrategy == 4) {
            return "OpenNF";
        } else if (ccStrategy == 5) {
            return "CHC";
        } else if (ccStrategy == 6) {
            return "S6";
        } else if (ccStrategy == 7) {
            return "Adaptive";
        } else {
            return "Invalid";
        }
    }

}
