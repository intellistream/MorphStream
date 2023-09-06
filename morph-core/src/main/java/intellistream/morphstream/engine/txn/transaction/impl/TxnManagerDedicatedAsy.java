package intellistream.morphstream.engine.txn.transaction.impl;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateAccessDescription;
import intellistream.morphstream.api.utils.ClientSideMetaTypes;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
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
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Condition;
import intellistream.morphstream.engine.txn.transaction.function.Function;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    public TxnManagerDedicatedAsy(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, int numberOfStates, String schedulerType) {
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
    public void SwitchScheduler(String schedulerType, int threadId, long markId) {
        currentSchedulerType.put(threadId, schedulerType);
        if (threadId == 0) {
            scheduler = schedulerPool.get(schedulerType);
            log.info("Current Scheduler is " + schedulerType + " markId: " + markId);
        }
    }
    @Override
    public void switch_scheduler(int thread_Id, long mark_ID) {
        if (scheduler instanceof RScheduler) {
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
            String schedulerType = collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id, mark_ID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
    }

    public void start_evaluate(int taskId, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean submitStateAccess(StateAccess stateAccess, TxnContext txnContext) {
        ClientSideMetaTypes.AccessType accessType = stateAccess.getAccessType();
        if (accessType == ClientSideMetaTypes.AccessType.MODIFY) {
        }
        return false;
    }

    public boolean Asy_ModifyRecord(StateAccess stateAccess, TxnContext txnContext) {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_COND;
        return false;
    }

    //TODO: Never used
    public abstract boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    //TODO: Never used
    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, List<DataBox> value, double[] enqueue_time) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
            }
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, long value, int column_id) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, primary_key, srcTable, t_record, value));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, primary_key, srcTable, t_record, value));
            }
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    //TODO: Never used
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
            }
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    //TODO: Never used
    @Override
    public boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String primary_key, TableRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READS_ONLY;//read multiple versions.
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable));
            }
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        source_key, s_record, s_record, function, column_id));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        source_key, s_record, s_record, function, column_id));
            }
        } else {
            if (enable_log) log.info("No record is found:" + source_key);
            return false;
        }
    }

    // Modify for deposit
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, 1));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, 1));
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, int[] success) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[1];
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        condition_records[0] = s_record;
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, null, new String[]{srcTable}, new String[]{key}, condition_records, condition, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, null, new String[]{srcTable}, new String[]{key}, condition_records, condition, success));
            }
        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_AST
    public boolean Asy_ModifyRecord(TxnContext txn_context,
                                    String srcTable, String key,
                                    Function function,
                                    String[] condition_sourceTable, String[] condition_source,
                                    Condition condition,
                                    int[] success) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, null, condition_sourceTable, condition_source, condition_records, condition, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, null, condition_sourceTable, condition_source, condition_records, condition, success));
            }
        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_READ;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref));
            }
        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, Condition condition, int[] success) throws DatabaseException {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_READ;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition, success));
            }
        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_ACT
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                         Function function,
                                         String[] condition_sourceTable, String[] condition_source,
                                         Condition condition, int[] success) throws DatabaseException {

        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_COND_READ;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                if (enable_log) log.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success));
            }
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_ACT
    public boolean Asy_ModifyRecord_ReadN(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                          Function function, String[] condition_sourceTable, String[] condition_source, int[] success) throws DatabaseException {

        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ_WRITE_COND_READN;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                if (enable_log) log.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            }
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // Window
    public boolean Asy_WindowReadRecords(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                         Function function, String[] condition_sourceTable, String[] condition_source, int[] success) throws DatabaseException {

        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WINDOWED_READ_ONLY;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                if (enable_log) log.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            }
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_ACT
    public boolean Asy_ModifyRecord_Non_ReadN(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                              Function function, String[] condition_sourceTable, String[] condition_source, int[] success) throws DatabaseException {

        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.NON_READ_WRITE_COND_READN;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        BaseTable[] tables = new BaseTable[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            tables[i] = storageManager_.getTable(condition_sourceTable[i]);
            if (condition_records[i] == null) {
                if (enable_log) log.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            if (enableGroup) {
                return schedulerByGroup.get(getGroupId(txn_context.thread_Id)).SubmitRequest(context, new Request(txn_context, tables, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            } else {
                return scheduler.SubmitRequest(context, new Request(txn_context, tables, accessType, srcTable,
                        key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));
            }
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    public void BeginTransaction(TxnContext txn_context) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitBegin(context);
        } else {
            scheduler.TxnSubmitBegin(context);
        }
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitFinished(context);
        } else {
            scheduler.TxnSubmitFinished(context);
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
