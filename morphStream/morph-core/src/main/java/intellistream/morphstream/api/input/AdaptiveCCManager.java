package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveCCManager {
    private final LinkedBlockingQueue<byte[]> monitorQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<byte[]> partitionQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<byte[]> cacheQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<byte[]> offloadQueue = new LinkedBlockingQueue<>();
    private static final ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = new ConcurrentHashMap<>(); //round-robin input queues for each executor (combo/bolt)
    private static final int writeThreadPoolSize = 4; //TODO: Hardcoded

    public AdaptiveCCManager() {
        MorphStreamEnv morphStreamEnv = MorphStreamEnv.get();
        Configuration configuration = morphStreamEnv.configuration();
        int _spoutNum = MorphStreamEnv.get().configuration().getInt("spoutNum"); //Number of thread for TPG_CC
        for (int i = 0; i < _spoutNum; i++) {
            BlockingQueue<TransactionalEvent> inputQueue = new LinkedBlockingQueue<>();
            tpgQueues.put(i, inputQueue);
        }
    }

    public void initialize() throws IOException {
        Thread listenerThread = new Thread(new SocketListener(monitorQueue, partitionQueue, cacheQueue, offloadQueue, tpgQueues));
        Thread monitorThread = new Thread(new MonitorThread(monitorQueue, 100));
        Thread partitionCCThread = new Thread(new PartitionCCThread(partitionQueue));
        Thread cacheCCThread = new Thread(new CacheCCThread(cacheQueue));
        Thread offloadCCThread = new Thread(new OffloadCCThread(offloadQueue, writeThreadPoolSize));

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
