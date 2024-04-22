package intellistream.morphstream.api.input;

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
    private static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private static final int writeThreadPoolSize = 4; //TODO: Hardcoded
    private static final HashMap<Integer, Integer> saTypeMap = new HashMap<>(); //State access ID -> state access type
    private static final HashMap<Integer, String> saTableNameMap = new HashMap<>(); //State access ID -> table name

    public AdaptiveCCManager() {
        int _spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum"); //Number of thread for TPG_CC
        for (int i = 0; i < _spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgQueues.put(i, inputQueue);
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
        Thread partitionCCThread = new Thread(new PartitionCCThread(partitionQueue));
        Thread cacheCCThread = new Thread(new CacheCCThread(cacheQueue));
        Thread offloadCCThread = new Thread(new OffloadCCThread(offloadQueue, writeThreadPoolSize, saTypeMap, saTableNameMap));

        listenerThread.start();
        monitorThread.start();
        partitionCCThread.start();
        cacheCCThread.start();
        offloadCCThread.start();
    }

    public BlockingQueue<TransactionalEvent> getInputQueue(int spoutId) {
        return tpgQueues.get(spoutId);
    }

}
