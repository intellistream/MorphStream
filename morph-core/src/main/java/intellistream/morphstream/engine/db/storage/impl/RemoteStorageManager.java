package intellistream.morphstream.engine.db.storage.impl;

import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import org.apache.log4j.Logger;

public class RemoteStorageManager {
    private static final Logger LOG = Logger.getLogger(RemoteStorageManager.class);
    private CacheBuffer cacheBuffer;
    public RemoteStorageManager(CacheBuffer cacheBuffer) {
        this.cacheBuffer = cacheBuffer;
    }

    public void initRemoteDataBase() {
        LOG.info("Initializing remote database");
    }

    public int read(String tableName, String key, int workerId) {
        return this.cacheBuffer.readCache(tableName, key, workerId);
    }

    public void write(String tableName, String key, int workerId) {
        LOG.info("Writing to remote database");
    }

}
