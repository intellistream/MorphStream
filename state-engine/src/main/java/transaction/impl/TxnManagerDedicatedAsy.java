package transaction.impl;

import content.common.CommonMetaTypes;
import content.common.CommonMetaTypes.AccessType;
import db.DatabaseException;
import lock.OrderLock;
import lock.PartitionedOrderLock;
import scheduler.Request;
import scheduler.context.BFSLayeredTPGContext;
import scheduler.context.DFSLayeredTPGContext;
import scheduler.context.GSTPGContext;
import scheduler.context.SchedulerContext;
import storage.*;
import storage.datatype.DataBox;
import transaction.TxnManager;
import transaction.context.TxnAccess;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.enable_log;
import static content.common.CommonMetaTypes.kMaxAccessNum;

/**
 * TxnManagerDedicated is a thread-local structure.
 */
@lombok.extern.slf4j.Slf4j
public abstract class TxnManagerDedicatedAsy extends TxnManager {
    protected final StorageManager storageManager_;
    protected final String thisComponentId;
    public SchedulerContext context;

    protected TxnAccess.AccessList access_list_ = new TxnAccess.AccessList(kMaxAccessNum);
    protected boolean is_first_access_;
    protected long thread_count_;

    public TxnManagerDedicatedAsy(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, int numberOfStates, String schedulerType) {
        this.storageManager_ = storageManager;
        this.thisComponentId = thisComponentId;
        thread_count_ = thread_count;
        is_first_access_ = true;


        SCHEDULER_TYPE scheduler_type = SCHEDULER_TYPE.valueOf(schedulerType);
        switch (scheduler_type) {
            case BFS:
                context = new BFSLayeredTPGContext(thisTaskId, thread_count);
                scheduler.AddContext(thisTaskId, context);
                break;
            case DFS: // TODO
                context = new DFSLayeredTPGContext(thisTaskId, thread_count);
                scheduler.AddContext(thisTaskId, context);
                break;
            case GS: // TODO
                context = new GSTPGContext(thisTaskId, thread_count);
                scheduler.AddContext(thisTaskId, context);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + scheduler_type);
        }
//        LOG.info("Engine initialize:" + " Total Working Threads:" + tthread);
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

    public abstract boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, List<DataBox> value, double[] enqueue_time) throws DatabaseException {
        AccessType accessType = AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, long value, int column_id) throws DatabaseException {
        AccessType accessType = AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable));
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        AccessType accessType = AccessType.READ_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable, enqueue_time));
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String primary_key, TableRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        AccessType accessType = AccessType.READS_ONLY;//read multiple versions.
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable));
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
        if (s_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    source_key, s_record, s_record, function, column_id));
        } else {
            if (enable_log) log.info("No record is found:" + source_key);
            return false;
        }
    }

    // Modify for deposit
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    key, s_record, s_record, function, 1));
        } else {
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, int[] success) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[1];
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        condition_records[0] = s_record;
        if (s_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    key, s_record, s_record, function, null, new String[]{srcTable}, new String[]{key}, condition_records, condition, success));
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
        AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    key, s_record, s_record, function, null, condition_sourceTable, condition_source, condition_records, condition, success));
        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_READ;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    key, s_record, s_record, function, record_ref));
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

        AccessType accessType = AccessType.READ_WRITE_COND_READ;
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
            return scheduler.SubmitRequest(context, new Request(txn_context, accessType, srcTable,
                    key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success));
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    public void BeginTransaction(TxnContext txn_context) {
        scheduler.TxnSubmitBegin(context);
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {
        scheduler.TxnSubmitFinished(context);
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

}
