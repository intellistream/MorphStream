package transaction.impl;

import com.google.common.collect.Iterators;
import content.common.CommonMetaTypes;
import content.common.CommonMetaTypes.AccessType;
import db.DatabaseException;
import lock.OrderLock;
import lock.PartitionedOrderLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.Request;
import scheduler.context.*;
import scheduler.context.og.*;
import scheduler.context.op.OPNSContext;
import scheduler.context.op.OPNSAContext;
import scheduler.context.op.OPSContext;
import scheduler.context.op.OPSAContext;
import scheduler.impl.IScheduler;
import stage.Stage;
import storage.*;
import storage.datatype.DataBox;
import transaction.TxnManager;
import transaction.context.TxnAccess;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.enable_log;
import static content.common.CommonMetaTypes.kMaxAccessNum;

/**
 * TxnManagerDedicated is a thread-local structure.
 * It is initialized during the initialize method of each operator.
 */
public abstract class TxnManagerDedicatedAsy extends TxnManager {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerDedicatedAsy.class);

    protected final StorageManager storageManager_;
    protected final String thisComponentId;
    public HashMap<String, SchedulerContext> contexts;
    public SchedulerContext context;

    protected TxnAccess.AccessList access_list_ = new TxnAccess.AccessList(kMaxAccessNum);
    protected boolean is_first_access_;
    protected int thread_count_;

    protected int dalta;

    public TxnManagerDedicatedAsy(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, int numberOfStates, String schedulerType, Stage stage) {
        super(stage);
        this.storageManager_ = storageManager;
        this.thisComponentId = thisComponentId;
        thread_count_ = thread_count;
        is_first_access_ = true;
        contexts = new HashMap<>();

        this.setSchedulerContext(thisTaskId, thread_count, schedulerType, stage.getScheduler(), stage);
        context = contexts.get(schedulerType);

    }

    public void setSchedulerContext(int thisTaskId, int thread_count, String schedulerType, IScheduler scheduler, Stage stage) {
        SCHEDULER_TYPE scheduler_type = SCHEDULER_TYPE.valueOf(schedulerType);
        SchedulerContext schedulerContext;
        switch (scheduler_type) {
            case OG_BFS:
            case OG_DFS:
                schedulerContext = new OGSContext(thisTaskId, thread_count, stage);
                break;
            case OG_BFS_A:
            case OG_DFS_A:
                schedulerContext = new OGSAContext(thisTaskId, thread_count, stage);
                break;
            case OG_NS:
            case TStream: // original tstream is the same as using GS scheduler..
                schedulerContext = new OGNSContext(thisTaskId, thread_count, stage);
                break;
            case OG_NS_A:
                schedulerContext = new OGNSAContext(thisTaskId, thread_count, stage);
                break;
            case OP_NS:
                schedulerContext = new OPNSContext(thisTaskId, stage);
                break;
            case OP_NS_A:
                schedulerContext = new OPNSAContext(thisTaskId, stage);
                break;
            case OP_BFS:
            case OP_DFS:
                schedulerContext = new OPSContext(thisTaskId, stage);
                break;
            case OP_BFS_A:
            case OP_DFS_A:
                schedulerContext = new OPSAContext(thisTaskId, stage);
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

    public void start_evaluate(int taskId, double mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
    }

    public abstract boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    public void InsertNewRecord(String table_name, String key, SchemaRecord record) throws DatabaseException {
//        AccessType type = AccessType.INSERT_ONLY;
        if (storageManager_.getTable(table_name).SelectKeyRecord(key) == null) { //Only insert if no existing record matching with input primary_key
            TableRecord tableRecord = new TableRecord(record);
            storageManager_.InsertRecord(table_name, tableRecord);
        }
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, List<DataBox> value, double[] enqueue_time, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable, enqueue_time));

        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, long value, int column_id, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.WRITE_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, primary_key, srcTable, t_record, value));

        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    //TODO: Added primary key & s_record in Request construction.
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_ONLY;
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable, primary_key, t_record, enqueue_time));

        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String primary_key, TableRecordRef record_ref, double[] enqueue_time, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READS_ONLY;//read multiple versions.
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
        if (t_record != null) {


            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable));

        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    source_key, s_record, s_record, function, column_id));

        } else {
            if (enable_log) log.info("No record is found:" + source_key);
            return false;
        }
    }

    // Modify for deposit
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, 1));

        } else {
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, int[] success, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[1];
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        condition_records[0] = s_record;
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, null, new String[]{srcTable}, new String[]{key}, condition_records, condition, success));

        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_AST, ED_TR, ED_WU, ED_CU_Tweet
    public boolean Asy_ModifyRecord(TxnContext txn_context,
                                    String srcTable, String key,
                                    Function function,
                                    String[] condition_sourceTable, String[] condition_source,
                                    Condition condition,
                                    int[] success,
                                    String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
        }

//        if (Objects.equals(operator_name, "ed_wu")) {log.info("WU Checking S_Record...");}

        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
//            if (enable_log) log.info("Record is found:" + key);

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, null, condition_sourceTable, condition_source, condition_records, condition, success));

        } else {
//            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Iteration(TxnContext txn_context,
                                              String srcTable, String key,
                                              Function function,
                                              String[] condition_sourceTable, String[] condition_source,
                                              Condition condition,
                                              int[] success,
                                              String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_COND;

        //The 1st element in condition_sourceTable is the table to be iterated.
        Iterator<TableRecord> iterator = storageManager_.getTable(condition_sourceTable[0]).iterator();
        TableRecord[] condition_records = new TableRecord[Iterators.size(iterator)];

        //Pass the entire iteration_table to condition_records
        int i = 0;
        while (iterator.hasNext()) {
            condition_records[i] = iterator.next();
            i++;
        }

        //s_record: tweetRecord, d_record: clusterRecord whose similarity is the highest (determined in OpScheduler)
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, null, condition_sourceTable, condition_source, condition_records, condition, success));

        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // ED_CU
    public boolean Asy_ModifyRecord_Iteration_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                              Function function, String[] condition_sourceTable, String[] condition_source,
                                              Condition condition, int[] success, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_COND_READ;

        //The 1st element in condition_sourceTable is the table to be iterated.
        Iterator<TableRecord> iterator = storageManager_.getTable(condition_sourceTable[0]).iterator();
        TableRecord[] condition_records = new TableRecord[Iterators.size(iterator)];

        //Pass the entire iteration_table to condition_records
        int i = 0;
        while (iterator.hasNext()) {
            condition_records[i] = iterator.next();
            i++;
        }

        //s_record: tweetRecord, d_record: clusterRecord whose similarity is the highest (determined in OpScheduler)
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success));

        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_READ;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, record_ref));

        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, Condition condition, int[] success, String operator_name) throws DatabaseException {
        AccessType accessType = AccessType.READ_WRITE_READ;
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, record_ref, condition, success));

        } else {
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_ACT, ED_TC, ED_ES
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                         Function function,
                                         String[] condition_sourceTable, String[] condition_source,
                                         Condition condition, int[] success, String operator_name) throws DatabaseException {

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

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success));

        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    @Override // TRANSFER_ACT
    public boolean Asy_ModifyRecord_ReadN(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                          Function function, String[] condition_sourceTable, String[] condition_source, int[] success, String operator_name) throws DatabaseException {

        AccessType accessType = AccessType.READ_WRITE_COND_READN;
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

            return stage.getScheduler().SubmitRequest(context, new Request(txn_context, accessType, operator_name, srcTable,
                    key, s_record, s_record, function, record_ref, condition_sourceTable, condition_source, condition_records, success));

        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            if (enable_log) log.info("No record is found:" + key);
            return false;
        }
    }

    public void BeginTransaction(TxnContext txn_context) {

        stage.getScheduler().TxnSubmitBegin(context);

    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {

        stage.getScheduler().TxnSubmitFinished(context);

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

    public int getGroupId(int thisTaskId) {
        int groupId = thisTaskId / dalta;
        return groupId;
    }
}
