package intellistream.morphstream.engine.txn.transaction.impl;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSContext;
import intellistream.morphstream.engine.txn.scheduler.context.recovery.RSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.recovery.RScheduler;
import intellistream.morphstream.engine.txn.storage.*;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * TxnManagerDedicated is a thread-local structure.
 */
public abstract class TxnManagerDedicatedAsy extends TxnManager {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerDedicatedAsy.class);
    protected final String thisComponentId;
    public HashMap<String, SchedulerContext> contexts;
    public SchedulerContext context;

    protected TxnAccess.AccessList access_list_ = new TxnAccess.AccessList(CommonMetaTypes.kMaxAccessNum);
    protected boolean is_first_access_;
    protected int thread_count_;
    protected int dalta;
//    public final Stage stage; //Each stateful operator has its own Stage, encapsulating a scheduler and a SOURCE_CONTROL.

    public TxnManagerDedicatedAsy(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, int numberOfStates, String schedulerType) {
//    public TxnManagerDedicatedAsy(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, int numberOfStates, String schedulerType, Stage stage) {
//        this.stage = stage;
        this.storageManager_ = storageManager;
        this.thisComponentId = thisComponentId;
        thread_count_ = thread_count;
        is_first_access_ = true;
        contexts = new HashMap<>();
        if (enableGroup) {
            dalta = (int) Math.ceil(thread_count / (double) groupNum);
            this.setSchedulerContext(thisTaskId, thread_count / groupNum, schedulerTypeByGroup.get(thisTaskId / dalta), schedulerByGroup.get(thisTaskId / dalta));
            context = contexts.get(schedulerTypeByGroup.get(thisTaskId / dalta));
        } else if (enableDynamic) {
            for (Map.Entry<String, IScheduler> entry : schedulerPool.entrySet()) {
                this.setSchedulerContext(thisTaskId, thread_count, entry.getKey(), entry.getValue());
            }
            context = contexts.get(schedulerType);
        } else {
            this.setSchedulerContext(thisTaskId, thread_count, schedulerType, scheduler);
            context = contexts.get(schedulerType);
        }
        if (recoveryScheduler != null) {
            this.setSchedulerContext(thisTaskId, thread_count, "Recovery", recoveryScheduler);
            context = contexts.get("Recovery");
        }
//        LOG.info("Engine initialize:" + " Total Working Threads:" + tthread);
    }

    public void setSchedulerContext(int thisTaskId, int thread_count, String schedulerType, IScheduler scheduler) {
        SCHEDULER_TYPE scheduler_type = SCHEDULER_TYPE.valueOf(schedulerType);
        SchedulerContext schedulerContext;
        switch (scheduler_type) {
            case OG_BFS:
            case OG_DFS:
                schedulerContext = new OGSContext(thisTaskId, thread_count);
                break;
            case OG_BFS_A:
            case OG_DFS_A:
                schedulerContext = new OGSAContext(thisTaskId, thread_count);
                break;
            case OG_NS:
            case TStream: // original tstream is the same as using GS scheduler..
                schedulerContext = new OGNSContext(thisTaskId, thread_count);
                break;
            case OG_NS_A:
                schedulerContext = new OGNSAContext(thisTaskId, thread_count);
                break;
            case OP_NS:
                schedulerContext = new OPNSContext(thisTaskId);
                break;
            case OP_NS_A:
                schedulerContext = new OPNSAContext(thisTaskId);
                break;
            case OP_BFS:
            case OP_DFS:
                schedulerContext = new OPSContext(thisTaskId);
                break;
            case OP_BFS_A:
            case OP_DFS_A:
                schedulerContext = new OPSAContext(thisTaskId);
                break;
            case Recovery:
                schedulerContext = new RSContext(thisTaskId);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + scheduler_type);
        }
        contexts.put(schedulerType, schedulerContext);
        scheduler.AddContext(thisTaskId, schedulerContext);
    }

    public void switchContext(String schedulerType) {
        context = contexts.get(schedulerType);
    }
    /**
     * Switch scheduler every punctuation
     * When the workload changes and the scheduler is no longer applicable
     */
    public void SwitchScheduler(String schedulerType, int threadId, long markId, int batchID, String operatorID) {
        currentSchedulerType.put(threadId, schedulerType);
        if (threadId == 0) {
            scheduler = schedulerPool.get(schedulerType);
            log.info("Current Scheduler is " + schedulerType + " markId: " + markId);
            RuntimeMonitor.get().UPDATE_SCHEDULER(operatorID, batchID, schedulerType);
        }
    }
    @Override
    public void switch_scheduler(int thread_Id, long mark_ID, int batchID, String operatorID) {
        if (scheduler instanceof RScheduler) {
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
            String schedulerType = collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id, mark_ID, batchID, operatorID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
    }

    public void start_evaluate(String operatorID, int batchID, int num_events, int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean submitStateAccess(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
        String accessType = stateAccess[0];
        if (accessType == "READ") {
            return Asy_ReadRecord(stateAccess, txnContext);
        } else if (accessType == "WRITE") {
            return Asy_WriteRecord(stateAccess, txnContext);
        } else if (accessType == "WINDOW_READ") {
            return Asy_WindowReadRecord(stateAccess, txnContext);
        } else if (accessType == "WINDOW_WRITE") {
            return Asy_WindowWriteRecord(stateAccess, txnContext);
        } else if (accessType == "NON_DETER_READ") {
            return Asy_NonDeterReadRecord(stateAccess, txnContext);
        } else if (accessType == "NON_DETER_WRITE") {
            return Asy_NonDeterWriteRecord(stateAccess, txnContext);
        } else {
            throw new UnsupportedOperationException("Unsupported access type: " + accessType);
        }
    }

    //If read only, set src key and table to read key, and add this single read access into readRecords.
    public boolean Asy_ReadRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
//        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ;
//        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
//        if (stateObjects.size() != 1) {
//            throw new UnsupportedOperationException("Read only supports single read access.");
//        }
//        StateObject stateObj = stateObjects.get(0);
//        String srcTable = stateObj.getTable();
//        String srcKey = stateObj.getKey();
//        TableRecord readRecord = storageManager_.getTable(srcTable).SelectKeyRecord(srcKey);
//
//        String[] condition_sourceTables = {srcTable};
//        String[] condition_sourceKeys = {srcKey};
//        HashMap<String, TableRecord> condition_records = new HashMap<>();
//        condition_records.put(stateObj.getName(), readRecord);
//
//        if (readRecord != null) {
//            if (enableGroup) {
//                return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//                //TODO: Replace with the following code to get scheduler by stage
////                return stage.getScheduler().SubmitRequest(context, new Request(txnContext, accessType, srcTable, srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            } else {
//                return scheduler.SubmitRequest(context, new Request(txnContext, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            }
//        } else {
//            if (enable_log) log.info("No record is found:" + srcKey);
//            return false;
//        }
        return false;
    }

    public boolean Asy_WriteRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WRITE;
        // stateAccess: type, writeObjIndex, [table name, key's value (updated with event data), field index in table, access type] * N

        int recordNum = (stateAccess.length - 2) / 4;
        int writeIndex = Integer.parseInt(stateAccess[1]);
        List<TableRecord> condition_records = new ArrayList<>();
        String[] condition_tables = new String[recordNum];
        String[] condition_keys = new String[recordNum];
        int[] condition_fieldIndexes = new int[recordNum];

        for (int i = 2; i < stateAccess.length; i += 4) {
            String table = stateAccess[i];
            String key = stateAccess[i + 1];
            int fieldIndex = Integer.parseInt(stateAccess[i + 2]);
            condition_tables[(i-2)/4] = table;
            condition_keys[(i-2)/4] = key;
            condition_fieldIndexes[(i-2)/4] = fieldIndex;
            TableRecord condition_record = storageManager_.getTable(table).SelectKeyRecord(key);
            if (condition_record != null) {
                condition_records.add(condition_record);
            } else {
                if (enable_log) log.info("No record is found:" + key);
                return false;
            }
        }

        String writeTable = stateAccess[writeIndex];
        String writeKey = stateAccess[writeIndex + 1];
        int writeFieldIndex = Integer.parseInt(stateAccess[writeIndex + 2]);
        TableRecord writeRecord = storageManager_.getTable(writeTable).SelectKeyRecord(writeKey);

        if (enableGroup) {
            return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, accessType, writeTable,
                    writeKey, writeFieldIndex, writeRecord, condition_tables, condition_keys, condition_fieldIndexes, condition_records, stateAccess));
        } else {
            return scheduler.SubmitRequest(context, new Request(txnContext, accessType, writeTable,
                    writeKey, writeFieldIndex, writeRecord, condition_tables, condition_keys, condition_fieldIndexes, condition_records, stateAccess));
        }
    }

    public boolean Asy_WindowReadRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
//        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WINDOW_READ;
//        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
//        if (stateObjects.size() != 1) {
//            throw new UnsupportedOperationException("Read only supports single read access.");
//        }
//        StateObject stateObj = stateObjects.get(0);
//        String srcTable = stateObj.getTable();
//        String srcKey = stateObj.getKey();
//        TableRecord readRecord = storageManager_.getTable(srcTable).SelectKeyRecord(srcKey);
//
//        String[] condition_sourceTables = {srcTable};
//        String[] condition_sourceKeys = {srcKey};
//        HashMap<String, TableRecord> condition_records = new HashMap<>();
//        condition_records.put(stateObj.getName(), readRecord);
//
//        if (readRecord != null) {
//            if (enableGroup) {
//                return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//                //TODO: Replace with the following code to get scheduler by stage
////                return stage.getScheduler().SubmitRequest(context, new Request(txnContext, accessType, srcTable, srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            } else {
//                return scheduler.SubmitRequest(context, new Request(txnContext, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            }
//        } else {
//            if (enable_log) log.info("No record is found:" + srcKey);
//            return false;
//        }
        return false;
    }

    public boolean Asy_WindowWriteRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
//        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WINDOW_WRITE;
//        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
//        HashMap<String, TableRecord> condition_records = new HashMap<>();
//
//        String[] condition_tables = new String[stateAccess.getStateObjects().size()];
//        String[] condition_keys = new String[stateAccess.getStateObjects().size()];
//
//        for (int i = 0; i < stateObjects.size(); i++) {
//            StateObject stateObj = stateObjects.get(i);
//            String stateObjTable = stateObj.getTable();
//            String stateObjKey = stateObj.getKey();
//
//            condition_tables[i] = stateObjTable;
//            condition_keys[i] = stateObjKey;
//            condition_records.put(stateObj.getName(), storageManager_.getTable(stateObjTable).SelectKeyRecord(stateObjKey));
//        }
//
//        StateObject writeStateObj = stateAccess.getStateObjectToWrite();
//        String writeTable = writeStateObj.getTable();
//        String writeKey = writeStateObj.getKey();
//        TableRecord writeRecord = storageManager_.getTable(writeTable).SelectKeyRecord(writeKey);
//
//        if (writeRecord != null) {
//            if (enableGroup) {
//                return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, accessType, writeTable,
//                        writeKey, writeRecord, condition_tables, condition_keys, condition_records, stateAccess));
//                //TODO: Replace with the following code to get scheduler by stage
////                return stage.getScheduler().SubmitRequest(context, new Request(txnContext, accessType, srcTable, srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            } else {
//                return scheduler.SubmitRequest(context, new Request(txnContext, accessType, writeTable,
//                        writeKey, writeRecord, condition_tables, condition_keys, condition_records, stateAccess));
//            }
//        } else {
//            if (enable_log) log.info("No record is found:" + writeKey);
//            return false;
//        }
        return false;
    }

    public boolean Asy_NonDeterReadRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
//        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.NON_DETER_READ;
//        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
//        if (stateObjects.size() != 1) {
//            throw new UnsupportedOperationException("Read only supports single read access.");
//        }
//        StateObject stateObj = stateObjects.get(0);
//        String srcTable = stateObj.getTable();
//        String srcKey = stateObj.getKey();
//        TableRecord readRecord = storageManager_.getTable(srcTable).SelectKeyRecord(srcKey);
//
//        BaseTable[] baseTables = {storageManager_.getTable(srcTable)};
//        String[] condition_sourceTables = {srcTable};
//        String[] condition_sourceKeys = {srcKey};
//        HashMap<String, TableRecord> condition_records = new HashMap<>();
//        condition_records.put(stateObj.getName(), readRecord);
//
//        if (readRecord != null) {
//            if (enableGroup) {
//                return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, baseTables, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//                //TODO: Replace with the following code to get scheduler by stage
////                return stage.getScheduler().SubmitRequest(context, new Request(txnContext, accessType, srcTable, srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            } else {
//                return scheduler.SubmitRequest(context, new Request(txnContext, baseTables, accessType, srcTable,
//                        srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            }
//        } else {
//            if (enable_log) log.info("No record is found:" + srcKey);
//            return false;
//        }
        return false;
    }

    public boolean Asy_NonDeterWriteRecord(String[] stateAccess, TxnContext txnContext) throws DatabaseException {
//        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.NON_DETER_WRITE;
//        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
//        HashMap<String, TableRecord> condition_records = new HashMap<>();
//        BaseTable[] baseTables = new BaseTable[stateObjects.size()];
//
//        String[] condition_tables = new String[stateAccess.getStateObjects().size()];
//        String[] condition_keys = new String[stateAccess.getStateObjects().size()];
//
//        for (int i = 0; i < stateObjects.size(); i++) {
//            StateObject stateObj = stateObjects.get(i);
//            String stateObjTable = stateObj.getTable();
//            String stateObjKey = stateObj.getKey();
//
//            condition_tables[i] = stateObjTable;
//            condition_keys[i] = stateObjKey;
//            condition_records.put(stateObj.getName(), storageManager_.getTable(stateObjTable).SelectKeyRecord(stateObjKey));
//            baseTables[i] = storageManager_.getTable(stateObjTable);
//        }
//
//        StateObject writeStateObj = stateAccess.getStateObjectToWrite();
//        String writeTable = writeStateObj.getTable();
//        String writeKey = writeStateObj.getKey();
//        TableRecord writeRecord = storageManager_.getTable(writeTable).SelectKeyRecord(writeKey);
//
//        if (writeRecord != null) {
//            if (enableGroup) {
//                return schedulerByGroup.get(getGroupId(txnContext.thread_Id)).SubmitRequest(context, new Request(txnContext, baseTables, accessType, writeTable,
//                        writeKey, writeRecord, condition_tables, condition_keys, condition_records, stateAccess));
//                //TODO: Replace with the following code to get scheduler by stage
////                return stage.getScheduler().SubmitRequest(context, new Request(txnContext, accessType, srcTable, srcKey, readRecord, condition_sourceTables, condition_sourceKeys, condition_records, stateAccess));
//            } else {
//                return scheduler.SubmitRequest(context, new Request(txnContext, baseTables, accessType, writeTable,
//                        writeKey, writeRecord, condition_tables, condition_keys, condition_records, stateAccess));
//            }
//        } else {
//            if (enable_log) log.info("No record is found:" + writeKey);
//            return false;
//        }
        return false;
    }


    public void BeginTransaction(TxnContext txn_context) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitBegin(context);
            //TODO: Replace with the following code for stage
//            stage.getScheduler().TxnSubmitBegin(context);
        } else {
            scheduler.TxnSubmitBegin(context);
        }
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context, int batchID) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitFinished(context, batchID);
            //TODO: Replace with the following code for stage
//            stage.getScheduler().TxnSubmitFinished(context);
        } else {
            scheduler.TxnSubmitFinished(context, batchID);
        }
        return true;
    }

    @Override
    public SchedulerContext getSchedulerContext() {
        return context;
    }

    //Below are not used by Asy Txn Manager.
    @Override
    public boolean SelectKeyRecord(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lock_ahead(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }


    public int getGroupId(int thisTaskId) {
        int groupId = thisTaskId / dalta;
        return groupId;
    }
}
