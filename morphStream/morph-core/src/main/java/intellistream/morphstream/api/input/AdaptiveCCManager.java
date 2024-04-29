package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.simVNF.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveCCManager {
    private final LinkedBlockingQueue<PatternData> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<PartitionData> partitionQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<CacheData> cacheQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<OffloadData> offloadQueue = new LinkedBlockingQueue<>();
    public static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private static final HashMap<Integer, Integer> saTypeMap = new HashMap<>(); //State access ID -> state access type
    private static final HashMap<Integer, String> saTableNameMap = new HashMap<>(); //State access ID -> table name
    private final HashMap<Integer, Integer> partitionOwnership = new HashMap<>(); //Maps each state partition to its current owner VNF instance.
    private final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private final int writeThreadPoolSize = MorphStreamEnv.get().configuration().getInt("offloadCCThreadNum");
    private final int tableSize = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int requestCounter = MorphStreamEnv.get().configuration().getInt("totalEvents");
    private final int pattern = MorphStreamEnv.get().configuration().getInt("workloadPattern");
    private final int ccStrategy = MorphStreamEnv.get().configuration().getInt("ccStrategy");


    public AdaptiveCCManager() {
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

    public void updateSATypeMap(int saID, int saType) {
        saTypeMap.put(saID, saType);
    }
    public void updateSATableNameMap(int saID, String tableName) {
        saTableNameMap.put(saID, tableName);
    }

    public void initialize() throws IOException {
        Thread listenerThread = new Thread(new SocketListener(monitorQueue, partitionQueue, cacheQueue, offloadQueue, tpgQueues));
        Thread monitorThread = new Thread(new MonitorThread(monitorQueue, 1000000));
        Thread partitionCCThread = new Thread(new PartitionCCThread(partitionQueue, partitionOwnership));
        Thread cacheCCThread = new Thread(new CacheCCThread(cacheQueue));
        Thread offloadCCThread = new Thread(new OffloadCCThread(offloadQueue, writeThreadPoolSize, saTypeMap, saTableNameMap, requestCounter));

        listenerThread.start();
        monitorThread.start();
        partitionCCThread.start();
        cacheCCThread.start();
        offloadCCThread.start();
    }

    public void startSimVNF() {//TODO: For manager-only testing
        VNFManager simVNF = new VNFManager(requestCounter, vnfInstanceNum, tableSize, ccStrategy, pattern);
        simVNF.startSimVNF();
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int spoutId) {
        return tpgQueues.get(spoutId);
    }

}
