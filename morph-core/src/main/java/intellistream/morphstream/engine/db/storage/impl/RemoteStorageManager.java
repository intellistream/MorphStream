package intellistream.morphstream.engine.db.storage.impl;

import intellistream.morphstream.api.input.statistic.WorkerSideOwnershipTable;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
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
import intellistream.morphstream.engine.txn.scheduler.context.ds.OCCContext;
import intellistream.morphstream.engine.txn.scheduler.context.ds.RLContext;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteStorageManager extends StorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteStorageManager.class);
    private final RemoteCallLibrary remoteCallLibrary = new RemoteCallLibrary();
    private final String[] tableNames;
    private final ConcurrentHashMap<String, Integer> tableNameToLength;
    public ConcurrentHashMap<String, Integer> tableNameToItemNumber = new ConcurrentHashMap<>();
    public CacheBuffer cacheBuffer;
    public ConcurrentHashMap<String, String> tempValueForTables = new ConcurrentHashMap<>();
    public int totalWorker;
    public int totalThread;
    public AtomicBoolean ownershipTableReady = new AtomicBoolean(false);
    public final ConcurrentHashMap<String, WorkerSideOwnershipTable> workerSideOwnershipTables = new ConcurrentHashMap<>();//tableName -> WorkerSideOwnershipTable
    public RemoteStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        this.cacheBuffer = cacheBuffer;
        this.tableNames = this.cacheBuffer.getTableNames();
        this.totalWorker = totalWorker;
        this.totalThread = totalThread;
        this.tableNameToLength = this.cacheBuffer.getTableNameToLength();
        for (String tableName: tableNames) {
            int length = this.cacheBuffer.getTableNameToLength().get(tableName);
            this.tempValueForTables.put(tableName, padStringToLength(tableName, length));
            workerSideOwnershipTables.put(tableName, new WorkerSideOwnershipTable(totalWorker));
            tableNameToItemNumber.put(tableName, MorphStreamEnv.get().configuration().getInt(tableName + "_num_items"));
        }
    }

    public void getOwnershipTable(RdmaWorkerManager rdmaWorkerManager, DSContext context) throws Exception {
        WorkerSideOwnershipTable workerSideOwnershipTable;
        if (this.ownershipTableReady.compareAndSet(false, true)) {
            for (String tableName: tableNames) {
                workerSideOwnershipTable = this.workerSideOwnershipTables.get(tableName);
                while (workerSideOwnershipTable.ownershipTableBuffer == null) {
                    workerSideOwnershipTable.ownershipTableBuffer = rdmaWorkerManager.getTableBuffer().getOwnershipTable();
                }
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
                        String key = new String(keyBytes, StandardCharsets.UTF_8);
                        workerSideOwnershipTable.putEachOwnership(key, workerId, index);
                        if (workerId == rdmaWorkerManager.getManagerId()) {
                            workerSideOwnershipTable.putEachKeyForThisWorker(key);
                        }
                    }
                }
                workerSideOwnershipTable.initValueList();
            }
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.WorkerRdmaRecvOwnershipTableEndEventTime(context.thisThreadId);
        MeasureTools.WorkerPrepareCacheStartTime(context.thisThreadId);
        this.loadCache(context, rdmaWorkerManager);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (context.thisThreadId < this.tableNames.length) {
            String tableName = this.tableNames[context.thisThreadId];
            while (!this.workerSideOwnershipTables.get(tableName).isFinishLoadValue()) {}
            this.cacheBuffer.initLocalCacheBuffer(this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker(), this.workerSideOwnershipTables.get(tableName).valueList, tableName);
        }
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.WorkerPrepareCacheEndTime(context.thisThreadId);
    }
    public void loadCache(DSContext context, RdmaWorkerManager rdmaWorkerManager) throws Exception {
       for (String tableName : tableNames) {
           List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
           int interval = (int) Math.floor((double) keys.size() / totalThread);
           int start = interval * context.thisThreadId;
           int end;
           if (context.thisThreadId == totalThread - 1) {
               end = keys.size();
           } else {
               end = interval * (context.thisThreadId + 1);
           }
           for (int i = start; i < end; i++) {
               String key = keys.get(i);
               this.readRemoteDatabaseForCache(tableName, key, rdmaWorkerManager, i, this.workerSideOwnershipTables.get(tableName).valueList, this.workerSideOwnershipTables.get(tableName).getTotalKeys());
           }
       }
    }
    public void updateWriteOwnership(String tableName, String key, int ownershipWorkerId, int threadId) {
        try {
            this.cacheBuffer.updateWriteOwnership(tableName, key, ownershipWorkerId, threadId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void updateSharedOwnership(String tableName, String key, int threadId, int numberToRead, int bigestBid) {
        try {
            this.cacheBuffer.updateSharedOwnership(tableName, key, threadId, numberToRead,bigestBid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean checkExclusiveOwnership(String tableName, String key, int threadId) {
        try {
            return this.cacheBuffer.checkExclusiveOwnership(tableName, key, threadId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean checkSharedOwnership(String tableName, String key, int threadId) {
        try {
            return this.cacheBuffer.checkSharedOwnership(tableName, key, threadId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public String directReadLocalCache(String tableName, String key, int tthread) {
        try {
            return this.cacheBuffer.directReadCache(tableName, key, tthread);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void asyncReadRemoteCacheForExclusiveLock(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, Operation.RemoteObject remoteObject)  {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 8);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.asyncReadRemoteCacheForExclusiveLock(workerId, keyIndex, tableIndex, this.tableNameToLength.get(tableName) + 8, remoteObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void asyncReadRemoteCacheForSharedLock(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, Operation.RemoteObject remoteObject)  {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 8);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.asyncReadRemoteCacheForSharedLock(workerId, keyIndex, tableIndex, this.tableNameToLength.get(tableName) + 8, remoteObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void asyncWriteRemoteCache(RdmaWorkerManager rdmaWorkerManager, String tableName, String key, String value) {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 8);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.asyncWriteRemoteCache(workerId, keyIndex, tableIndex, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void asyncSharedLockRelease(RdmaWorkerManager rdmaWorkerManager, String tableName, String key) {
        int keyIndex = 0;
        int tableIndex = 0;
        for (int i = 0; i < this.tableNames.length; i ++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                break;
            }
        }
        keyIndex = keyIndex + this.workerSideOwnershipTables.get(tableName).getOwnershipIndex(key) * (this.tableNameToLength.get(tableName) + 8);
        int workerId = this.workerSideOwnershipTables.get(tableName).getOwnershipWorkerId(key);
        try {
            rdmaWorkerManager.asyncSharedLockRelease(workerId, keyIndex, tableIndex, 8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void writeRemoteDatabase(String tableName, String key, int workerId) {
        //remoteCallLibrary.write(tableName, key, workerId);
    }
    public void readRemoteDatabaseForCache(String tableName, String key, RdmaWorkerManager rdmaWorkerManager, int valueIndex, String[] valueList, AtomicInteger count) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8) + 8;
                size = this.tableNameToLength.get(tableName);
                break;
            }
        }
        rdmaWorkerManager.asyncReadRemoteForCache(keyIndex, tableIndex, size, valueIndex, valueList, count);
    }
    public boolean exclusiveLockAcquisition(long bid, String tableName, String key, RdmaWorkerManager rdmaWorkerManager, RLContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 8;
                break;
            }
        }
        return rdmaWorkerManager.exclusiveLockAcquisition(bid, keyIndex, tableIndex, size, remoteObject);
    }
    public void exclusiveLockRelease(String tableName, String key, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 4;
                break;
            }
        }
        rdmaWorkerManager.exclusiveLockRelease(keyIndex, tableIndex, size);
    }
    public void sharedLockAcquisition(String tableName, String key, RdmaWorkerManager rdmaWorkerManager, RLContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                if (Integer.parseInt(key) >= this.tableNameToItemNumber.get(tableName)) {
                    LOG.info("Key is out of range with key " + key + " and table " + tableName);
                    throw new DatabaseException("Key is out of range");
                }
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 8;
                break;
            }
        }
        rdmaWorkerManager.sharedLockAcquisition(keyIndex, tableIndex, size, remoteObject);
    }
    public void sharedLockRelease(String tableName, String key, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                if (Integer.parseInt(key) >= this.tableNameToItemNumber.get(tableName)) {
                    LOG.info("Key is out of range with key " + key + " and table " + tableName);
                    throw new DatabaseException("Key is out of range");
                }
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 8;
                break;
            }
        }
        rdmaWorkerManager.sharedLockRelease(keyIndex, tableIndex, size);
    }
    public void asyncReadRemoteDatabaseWithExclusiveLock(String tableName, String key, RdmaWorkerManager rdmaWorkerManager, RLContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = this.tableNameToLength.get(tableName) + 8;
                break;
            }
        }
        rdmaWorkerManager.asyncReadRemoteWithExclusiveLock(keyIndex, tableIndex, size, remoteObject);
    }
    public boolean syncReadRemoteDatabaseWithSharedLock(String tableName, String key, RdmaWorkerManager rdmaWorkerManager, RLContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 8 + this.tableNameToLength.get(tableName);
                break;
            }
        }
        return rdmaWorkerManager.syncReadRemoteDatabaseWithSharedLock(keyIndex, tableIndex, size, remoteObject);
    }
    public void asyncReadRemoteDatabaseWithVersion(String tableName, String key, RdmaWorkerManager rdmaWorkerManager, OCCContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = this.tableNameToLength.get(tableName) + 8;
                break;
            }
        }
        rdmaWorkerManager.asyncReadRemoteDatabaseWithVersion(keyIndex, tableIndex, size, remoteObject);
    }
    public boolean validationWriteLockAcquisition(long bid, String tableName, String key, RdmaWorkerManager rdmaWorkerManager, OCCContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(tableName) + 8);
                size = 8;
                break;
            }
        }
        return rdmaWorkerManager.validationLockAcquisition(bid, keyIndex, tableIndex, size, remoteObject);
    }
    public boolean validationReadAcquisition (String table, String key, RdmaWorkerManager rdmaWorkerManager, OCCContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(table)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(key) * (this.tableNameToLength.get(table) + 8);
                size = 8;
                break;
            }
        }
        return rdmaWorkerManager.validationReadAcquisition(keyIndex, tableIndex, size, remoteObject);
    }
    public void asyncWriteRemoteDatabase(String tableName, String pKey, Object udfResult, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(pKey) * (this.tableNameToLength.get(tableName) + 8) + 8;
                size = this.tableNameToLength.get(tableName);
                break;
            }
        }
        rdmaWorkerManager.asyncWriteRemoteDatabase(keyIndex, tableIndex, size, udfResult);
    }

    public void asyncWriteRemoteDatabaseWithVersion(String tableName, String pKey, Object udfResult, RdmaWorkerManager rdmaWorkerManager, OCCContext.RemoteObject remoteObject) throws Exception {
        int tableIndex = 0;
        int keyIndex = 0;
        int size = 0;
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i].equals(tableName)) {
                tableIndex = i;
                keyIndex = Integer.parseInt(pKey) * (this.tableNameToLength.get(tableName) + 8) + 4;
                size = this.tableNameToLength.get(tableName) + 4;//version + value
                break;
            }
        }
        rdmaWorkerManager.asyncWriteRemoteDatabaseWithVersion(keyIndex, tableIndex, size, udfResult, remoteObject);
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