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
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteStorageManager extends StorageManager {
    private static final Logger LOG = Logger.getLogger(RemoteStorageManager.class);
    private final RemoteCallLibrary remoteCallLibrary = new RemoteCallLibrary();
    private final String[] tableNames;
    private final ConcurrentHashMap<String, Integer> tableNameToLength;
    public CacheBuffer cacheBuffer;
    public ConcurrentHashMap<String, String> tempValueForTables = new ConcurrentHashMap<>();
    public int totalWorker;
    public int totalThread;
    public AtomicBoolean ownershipTableReady = new AtomicBoolean(false);
    public final ConcurrentHashMap<String, WorkerSideOwnershipTable> workerSideOwnershipTables = new ConcurrentHashMap<>();
    public RemoteStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        this.cacheBuffer = cacheBuffer;
        this.tableNames = this.cacheBuffer.getTableNames();
        this.totalWorker = totalWorker;
        this.totalThread = totalThread;
        this.tableNameToLength = this.cacheBuffer.getTableNameToLength();
        for (int i = 0; i < totalWorker; i++) {
            workerSideOwnershipTables.put(String.valueOf(i), new WorkerSideOwnershipTable(totalWorker));
        }
        for (String tableName: tableNames) {
            int length = this.cacheBuffer.getTableNameToLength().get(tableName);
            this.tempValueForTables.put(tableName, padStringToLength(tableName, length));
        }
    }

    public void getOwnershipTable(RdmaWorkerManager rdmaWorkerManager, DSContext context) throws IOException {
        WorkerSideOwnershipTable workerSideOwnershipTable;
        if (this.ownershipTableReady.compareAndSet(false, true)) {
            for (String tableName: tableNames) {
                LOG.info("Start to get ownership table for table " + tableName);
                workerSideOwnershipTable = this.workerSideOwnershipTables.get(tableName);
                while (workerSideOwnershipTable.ownershipTableBuffer == null) {
                    workerSideOwnershipTable.ownershipTableBuffer = rdmaWorkerManager.getTableBuffer().getOwnershipTable();
                }
                LOG.info("Start to receive ownership table for table " + tableName);
                int[] length = new int[totalWorker];
                for (int i = 0; i < totalWorker; i++) {
                    length[i] = workerSideOwnershipTable.ownershipTableBuffer.getInt();
                    workerSideOwnershipTable.putTotalKeysForWorker(i, length[i]);
                }
                for (int workerId = 0; workerId < workerSideOwnershipTable.getTotalWorker(); workerId ++) {
                    for (int index = 0; index < length[workerId]; index ++) {
                        int keyLength = workerSideOwnershipTable.ownershipTableBuffer.getInt();
                        byte[] keyBytes = new byte[keyLength];
                        workerSideOwnershipTable.ownershipTableBuffer.get(keyBytes);
                        String key = new String(keyBytes);
                        workerSideOwnershipTable.putEachOwnership(key, workerId, index);
                        if (workerId == rdmaWorkerManager.getManagerId()) {
                            workerSideOwnershipTable.putEachKeyForThisWorker(key);
                        }
                    }
                }
                workerSideOwnershipTable.initValueList();
                LOG.info("Get ownership table");
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.WorkerRdmaRecvOwnershipTableEndEventTime(context.thisThreadId);
        MeasureTools.WorkerPrepareCacheStartTime(context.thisThreadId);
        this.loadCache(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (context.thisThreadId == 0) {
            for (String tableName : tableNames) {
                this.cacheBuffer.initLocalCacheBuffer(this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker(), this.workerSideOwnershipTables.get(tableName).valueList, tableName);
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.WorkerPrepareCacheEndTime(context.thisThreadId);
    }
    public void loadCache(DSContext context) {
       for (String tableName : tableNames) {
           List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
           int interval = (int) Math.ceil((double) keys.size() / totalThread);
           int start = interval * context.thisThreadId;
           int end = Math.min(interval * (context.thisThreadId + 1), keys.size());
           for (int i = start; i < end; i++) {
               String key = keys.get(i);
               String value = this.readRemoteDatabase(tableName, key);
               this.workerSideOwnershipTables.get(tableName).valueList[i] = value;
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
    public String readLocalCache(String tableName, String key, int workerId) {
        return this.cacheBuffer.readCache(tableName, key, workerId);
    }
    public String syncReadRemoteCache(RdmaWorkerManager rdmaWorkerManager, String tableName, String key)  {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 2);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            return rdmaWorkerManager.syncReadRemoteCache(workerId, keyIndex, tableIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void syncWriteRemoteCache(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, String value) {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 2);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.syncWriteRemoteCache(workerId, keyIndex, tableIndex, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void writeRemoteDatabase(String tableName, String key, int workerId) {
        //remoteCallLibrary.write(tableName, key, workerId);
    }
    private String readRemoteDatabase(String tableName, String key) {
        return this.tempValueForTables.get(tableName);
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
    //Pad the string to a specified length, filling the insufficient parts with spaces.
    private String padStringToLength(String str, int length) {
        if (str.length() >= length) {
            return str.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() < length) {
            sb.append(' ');
        }
        return sb.toString();
    }
}
