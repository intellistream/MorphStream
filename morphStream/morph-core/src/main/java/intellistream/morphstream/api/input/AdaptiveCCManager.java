package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import message.VNFCtlStub;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveCCManager {
    private Thread monitorThread;
    private Thread partitionCCThread;
    private Thread cacheCCThread;
    private Thread offloadCCThread;
    private Thread openNFCCThread;
    private Thread chcThread;
    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<PartitionData> partitionQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<CacheData> cacheQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<OffloadData> offloadQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<OffloadData> openNFQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<OffloadData> chcQueue = new LinkedBlockingQueue<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    public static HashMap<Integer, VNFCtlStub> vnfStubs = new HashMap<>();
    private VNFManager vnfManager;
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private final int writeThreadPoolSize = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int totalRequests = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private final int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");
    private final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

    public AdaptiveCCManager() {
        monitorThread = new Thread(new MonitorThread(monitorQueue, 1000000));
        partitionCCThread = new Thread(new PartitionCCThread(partitionQueue, partitionOwnership));
        cacheCCThread = new Thread(new CacheCCThread(cacheQueue));
        offloadCCThread = new Thread(new OffloadCCThread(offloadQueue, writeThreadPoolSize));
        openNFCCThread = new Thread(new OpenNFController(openNFQueue));
        chcThread = new Thread(new CHCController(chcQueue));
        int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread"); //Number of thread for TPG_CC
        for (int i = 0; i < tpgThreadNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgQueues.put(i, inputQueue);
        }
        int partitionGap = tableSize / vnfInstanceNum;
        for (int i = 0; i < tableSize; i++) {
            partitionOwnership.put(i, i / partitionGap); //TODO: Refine this
        }
    }

    public void startCC123_Monitor() {
        monitorThread.start();
        partitionCCThread.start();
        cacheCCThread.start();
        offloadCCThread.start();
        System.out.println("CC123 and Monitor started");
    }

    public void startOpenNF() {
        openNFCCThread.start();
        System.out.println("OpenNF controller started");
    }

    public void startCHC() {
        chcThread.start();
        System.out.println("CHC controller started");
    }

    /** For java simulated VNF instances only */
    public void startVNFInstances() {
        vnfManager = new VNFManager(totalRequests, vnfInstanceNum, tableSize, ccStrategy, pattern);
        vnfManager.startVNFInstances();
    }

    /** For java simulated VNF instances only */
    public double joinVNFInstances() {
        return vnfManager.joinVNFInstances();
    }

    public BlockingQueue<TransactionalEvent> getTPGInputQueue(int spoutId) {
        return tpgQueues.get(spoutId);
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
        } else {
            return "invalid";
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
        } else {
            return "invalid";
        }
    }

}
