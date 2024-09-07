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
    private Thread replicationCCThread;
    private HashMap<Integer, Thread> offloadExecutorThreads = new HashMap<>();
    private Thread openNFThread;
    private Thread chcThread;
    private Thread s6Thread;
    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> partitionQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> replicationQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> openNFQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> chcQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<VNFRequest> s6Queue = new LinkedBlockingQueue<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<VNFRequest>> offloadingQueues = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private final int offloadCCThreadNum = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private final String ccStrategy = MorphStreamEnv.get().configuration().getString("ccStrategy");


    public UniversalStateManager() {
        monitorThread = new Thread(new BatchMonitorThread(monitorQueue));
        replicationCCThread = new Thread(new ReplicationStateManager(replicationQueue));
        openNFThread = new Thread(new OpenNFStateManager(openNFQueue));
        chcThread = new Thread(new CHCStateManager(chcQueue));
        s6Thread = new Thread(new S6StateManager(s6Queue));

        int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread"); //Number of thread for TPG_CC
        for (int i = 0; i < tpgThreadNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgQueues.put(i, inputQueue);
        }

        for (int i = 0; i < offloadCCThreadNum; i++) {
            BlockingQueue<VNFRequest> inputQueue = new LinkedBlockingQueue<>();
            offloadingQueues.put(i, inputQueue);
            Thread offloadExecutorThread = new Thread(new OffloadExecutorThread(i, inputQueue));
            offloadExecutorThreads.put(i, offloadExecutorThread);
        }

        int partitionGap = tableSize / vnfInstanceNum;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap); //TODO: Refine this
        }
    }

    public void startAdaptiveCC() {
        monitorThread.start();
        replicationCCThread.start();
        for (int i = 0; i < offloadCCThreadNum; i++) {
            offloadExecutorThreads.get(i).start();
        }
        System.out.println("CC123 and Monitor started");
    }

    public void joinAdaptiveCC() {
        try {
            monitorThread.join();
            replicationCCThread.join();
            for (int i = 0; i < offloadCCThreadNum; i++) {
                offloadExecutorThreads.get(i).join();
            }
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
        for (int i = 0; i < offloadCCThreadNum; i++) {
            offloadExecutorThreads.get(i).start();
        }
        System.out.println("Offload executors started");
    }

    public void joinOffloadExecutorThreads() {
        try {
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

}
