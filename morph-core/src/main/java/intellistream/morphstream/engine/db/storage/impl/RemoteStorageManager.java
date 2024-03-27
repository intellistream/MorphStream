package intellistream.morphstream.engine.db.storage.impl;

import intellistream.morphstream.api.input.statistic.WorkerSideOwnershipTable;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.impl.remote.RemoteCallLibrary;
import intellistream.morphstream.engine.db.storage.StorageManager;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class RemoteStorageManager extends StorageManager {
    private static final Logger LOG = Logger.getLogger(RemoteStorageManager.class);
    private final RemoteCallLibrary remoteCallLibrary = new RemoteCallLibrary();
    private String[] tableNames;
    public CacheBuffer cacheBuffer;
    public int totalWorker;
    public int totalThread;
    public final WorkerSideOwnershipTable workerSideOwnershipTable;
    public RemoteStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        this.cacheBuffer = cacheBuffer;
        this.tableNames = this.cacheBuffer.getTableNames();
        this.totalWorker = totalWorker;
        this.totalThread = totalThread;
        this.workerSideOwnershipTable = new WorkerSideOwnershipTable(totalWorker);
    }

    public void getOwnershipTable(RdmaWorkerManager rdmaWorkerManager, DSContext context) throws IOException {
        if (!this.workerSideOwnershipTable.ownershipTableReady.compareAndSet(false, true)) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        } else {
            LOG.info("Start to get ownership table");
            while (this.workerSideOwnershipTable.ownershipTableBuffer == null) {
                this.workerSideOwnershipTable.ownershipTableBuffer = rdmaWorkerManager.getTableBuffer().getOwnershipTable();
            }
            LOG.info("Start to receive ownership table");
            int[] length = new int[workerSideOwnershipTable.getTotalWorker()];
            for (int i = 0; i < workerSideOwnershipTable.getTotalWorker(); i++) {
                length[i] = workerSideOwnershipTable.ownershipTableBuffer.getInt();
                workerSideOwnershipTable.putTotalKeysForWorker(i, length[i]);
            }
            for (int i = 0; i < workerSideOwnershipTable.getTotalWorker(); i++) {
                for (int j = 0; j < length[i]; j++) {
                    int keyLength = workerSideOwnershipTable.ownershipTableBuffer.getInt();
                    byte[] keyBytes = new byte[keyLength];
                    workerSideOwnershipTable.ownershipTableBuffer.get(keyBytes);
                    String key = new String(keyBytes);
                    workerSideOwnershipTable.putEachOwnership(key, i, j);
                    if (i == rdmaWorkerManager.getManagerId()) {
                        workerSideOwnershipTable.putEachKeyForThisWorker(key);
                    }
                    for (String tableName : tableNames) {
                        workerSideOwnershipTable.initTableNameToValueList(tableName);
                    }
                }
            }
            LOG.info("Get ownership table");
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        }
        this.loadCache(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (context.thisThreadId == 0) {
            for (String tableName : tableNames) {
                this.cacheBuffer.initLocalCacheBuffer(this.workerSideOwnershipTable.getKeysForThisWorker(), this.workerSideOwnershipTable.tableNameToValueList.get(tableName), tableName);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }
    public void loadCache(DSContext context) {
       for (String tableName : tableNames) {
           List<String> keys = this.workerSideOwnershipTable.getKeysForThisWorker();
           int interval = (int) Math.ceil((double) keys.size() / totalThread);
           int start = interval * context.thisThreadId;
           int end = Math.min(interval * (context.thisThreadId + 1), keys.size());
           for (int i = start; i < end; i++) {
               String key = keys.get(i);
               int value = this.readRemoteDatabase(tableName, key);
               this.workerSideOwnershipTable.tableNameToValueList.get(tableName)[i] = value;
           }
           LOG.info("Thread " + context.thisThreadId + " load cache for table " + tableName + " from remote database");
       }
    }
    public void updateOwnership(String tableName, String key, int ownershipWorkerId) {
        this.cacheBuffer.updateOwnership(tableName, key, ownershipWorkerId);
    }
    public boolean checkOwnership(String tableName, String key) {
        return this.cacheBuffer.checkOwnership(tableName, key);
    }
    public int readLocalCache(String tableName, String key, int workerId, int signature) {
        return this.cacheBuffer.readCache(tableName, key, workerId, signature);
    }
    public int syncReadRemoteCache(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, int signature)  {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            keyIndex = keyIndex + this.workerSideOwnershipTable.workerIdToTotalKeys.get(i) * 6;
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTable.getOwnershipIndex(key) * 6;
        int workerId = this.workerSideOwnershipTable.getOwnershipWorkerId(key);
        try {
            return rdmaWorkerManager.syncReadRemoteCache(workerId, keyIndex, tableIndex, signature);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void syncWriteRemoteCache(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, int value) {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            keyIndex = keyIndex + this.workerSideOwnershipTable.workerIdToTotalKeys.get(i) * 6;
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTable.getOwnershipIndex(key) * 6;
        int workerId = this.workerSideOwnershipTable.getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.syncWriteRemoteCache(workerId, keyIndex, tableIndex, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void writeRemoteDatabase(String tableName, String key, int workerId) {
        remoteCallLibrary.write(tableName, key, workerId);
    }
    private int readRemoteDatabase(String tableName, String key) {
        return 0;
    }

    @Override
    public void createTable(RecordSchema tableSchema, String tableName, int partitionNum, int numItems) {
        remoteCallLibrary.init();
    }
    @Override
    public void InsertRecord(String table, TableRecord record, int partitionId) throws DatabaseException {
        writeRemoteDatabase(table, record.record_.GetPrimaryKey(), partitionId);
    }

    @Override
    public void dropAllTables() {

    }

    @Override
    public void close() {

    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void syncReloadDatabase(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
