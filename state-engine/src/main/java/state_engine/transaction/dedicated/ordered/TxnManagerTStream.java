package state_engine.transaction.dedicated.ordered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.Meta.MetaTypes;
import state_engine.common.Operation;
import state_engine.storage.*;
import state_engine.storage.datatype.DataBox;
import state_engine.transaction.dedicated.TxnManagerDedicated;
import state_engine.transaction.function.Condition;
import state_engine.transaction.function.Function;
import state_engine.transaction.impl.TxnContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

import static state_engine.Meta.MetaTypes.AccessType.INSERT_ONLY;
import static state_engine.transaction.impl.TxnAccess.Access;


/**
 * conventional two-phase locking with no-sync_ratio strategy from Cavalia.
 */
public class TxnManagerTStream extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerTStream.class);
    TxnProcessingEngine instance;


    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int NUM_RECORDS, int thread_countw) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw);

        instance = TxnProcessingEngine.getInstance();
//        delta_long = (int) Math.ceil(NUM_ITEMS / (double) thread_countw);//range of each partition. depends on the number of op in the stage.

        delta = (int) Math.ceil(NUM_RECORDS / (double) thread_countw);//NUM_ITEMS / tthread;

//        switch (config.getInt("app")) {
//            case "StreamLedger": {
//                delta = (int) Math.ceil(NUM_ACCOUNTS / (double) thread_countw);//NUM_ITEMS / tthread;
//                break;
//            }
//            case "OnlineBiding": {
//                delta = (int) Math.ceil(NUM_ITEMS / (double) thread_countw);//NUM_ITEMS / tthread;
//                break;
//            }
//
//            case "TP": {
//                delta = (int) Math.ceil(NUM_SEGMENTS / (double) thread_countw);//NUM_ITEMS / tthread;
//                break;
//            }
//
//            case "MicroBenchmark": {
//                delta = (int) Math.ceil(NUM_ITEMS / (double) thread_countw);//NUM_ITEMS / tthread;
//                break;
//            }
//            case "PositionKeeping": {
//                delta = (int) Math.ceil(NUM_MACHINES / (double) thread_countw);//NUM_ITEMS / tthread;
//                break;
//            }
//        }

    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
//		BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            if (!tb_record.content_.TryWriteLock()) {
                this.AbortTransaction();//shall never be called.
                return false;
            } else {
//				LOG.info(tb_record.toString() + "is locked by insertor");
            }
            record.is_visible_ = true;
            Access access = access_list_.NewAccess();
            access.access_type_ = INSERT_ONLY;
            access.access_record_ = tb_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = 0;
//		END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
            return true;
        } else {
//				END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
            return true;
        }
    }


    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) {
        //not in use.

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean CommitTransaction(TxnContext txnContext) {
        //not in use.
        throw new UnsupportedOperationException();
    }


    @Override
    public void AbortTransaction() {
        throw new UnsupportedOperationException();
    }


    private int getTaskId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    /**
     * build the Operation chain.. concurrently..
     *
     * @param record      of interest
     * @param primaryKey
     * @param table_name
     * @param accessType  Read or Write @ notice that, in the original Cavalia's design, write is proceed as Read. That is, Read->Modify->Write as one Operation.
     * @param record_ref
     * @param txn_context
     */
    public void operation_chain_construction_read_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, SchemaRecordRef record_ref, TxnContext txn_context) {

        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        MyList<Operation> myList = holder.get(primaryKey);

//        LOG.info(String.valueOf(OsUtils.Addresser.addressOf(record_ref)));

        myList.add(new Operation(table_name, txn_context, bid, accessType, record, record_ref));


//        Integer key = Integer.valueOf(record.record_.GetPrimaryKey());
//        int taskId = getTaskId(key);
//        int h2ID = getH2ID(key);
//        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
//        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, record_ref));
    }

    public void operation_chain_construction_read_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, TableRecordRef record_ref, TxnContext txn_context) {

        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        MyList<Operation> myList = holder.get(primaryKey);

//        LOG.info(String.valueOf(OsUtils.Addresser.addressOf(record_ref)));

        myList.add(new Operation(table_name, txn_context, bid, accessType, record, record_ref));


//        Integer key = Integer.valueOf(record.record_.GetPrimaryKey());
//        int taskId = getTaskId(key);
//        int h2ID = getH2ID(key);
//        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
//        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, record_ref));
    }

    /**
     * @param record
     * @param primaryKey
     * @param table_name
     * @param bid
     * @param accessType
     * @param value
     * @param txn_context
     */
    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, List<DataBox> value, TxnContext txn_context) {

        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, value));

//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, value_list));
    }

    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, long value, int column_id, TxnContext txn_context) {

        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, value, column_id));

//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, value_list));
    }

    private void operation_chain_construction_modify_read(TableRecord record, String table_name, long bid,
                                                          MetaTypes.AccessType accessType, SchemaRecordRef record_ref, Function function, TxnContext txn_context) {
        String primaryKey = record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        //simple sequential build.
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, record_ref, function));

//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
//        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);

//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, record_ref, function));
    }


    private void operation_chain_construction_modify_only(TableRecord s_record, String table_name, long bid, MetaTypes.AccessType accessType, TableRecord d_record, Function function, TxnContext txn_context, int column_id) {
        String primaryKey = d_record.record_.GetPrimaryKey();

        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, s_record, d_record, bid, accessType, function, txn_context, column_id));

//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(s_record)).holder_v3;
//        holder.add(new Operation(s_record, d_record, bid, accessType, function, txn_context));
    }


    private void operation_chain_construction_modify_only(String table_name, long bid, MetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, Function function, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {

        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success));
//
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success));
    }

    private void operation_chain_construction_modify_only(String table_name, long bid, MetaTypes.AccessType accessType, TableRecord d_record, Function function, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {

        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, d_record, bid, accessType, function, condition_records, condition, txn_context, success));
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(fid).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(fid).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(d_record, bid, accessType, function, condition_records, condition, txn_context, success));

    }

    private void operation_chain_construction_modify_read(String table_name, long bid, MetaTypes.AccessType accessType, TableRecord d_record, SchemaRecordRef record_ref, Function function
            , TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {

        String primaryKey = d_record.record_.GetPrimaryKey();
        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
        holder.get(primaryKey).add(new Operation(table_name, d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success));

//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
//        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(fid).rangeMap.get(taskId).holder_v2[h2ID];
//        Set<Operation> holder = instance.getHolder(fid).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success));
    }

    /**
     * Build Operation chains during SP execution.
     *
     * @param txn_context
     * @param primary_key
     * @param table_name
     * @param t_record
     * @param record_ref
     * @param enqueue_time
     * @param accessType
     * @return
     */
    @Override
    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType accessType) {

        long bid = txn_context.getBID();

        operation_chain_construction_read_only(t_record, primary_key, table_name, bid, accessType, record_ref, txn_context);

        return true;//it should be always success.
    }

    /**
     * Build Operation chains during SP execution.
     *
     * @param txn_context
     * @param primary_key
     * @param table_name
     * @param t_record
     * @param record_ref
     * @param enqueue_time
     * @param accessType
     * @return
     */
    @Override
    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, TableRecordRef record_ref, double[] enqueue_time, MetaTypes.AccessType accessType) {

        long bid = txn_context.getBID();

        operation_chain_construction_read_only(t_record, primary_key, table_name, bid, accessType, record_ref, txn_context);

        return true;//it should be always success.
    }

    @Override
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, long value, int column_id, MetaTypes.AccessType access_type) {

        long bid = txn_context.getBID();

        operation_chain_construction_write_only(t_record, primary_key, table_name, bid, access_type, value, column_id, txn_context);

        return true;//it should be always success.
    }


    @Override
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, MetaTypes.AccessType access_type) {

        long bid = txn_context.getBID();

        operation_chain_construction_write_only(t_record, primary_key, table_name, bid, access_type, value, txn_context);

        return true;//it should be always success.
    }

    @Override
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord t_record, TableRecord d_record, Function function, MetaTypes.AccessType accessType, int column_id) {

        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(t_record, srcTable, bid, accessType, d_record, function, txn_context, column_id);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;
    }


    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord t_record,
                                              SchemaRecordRef record_ref, Function function, MetaTypes.AccessType accessType) {

        long bid = txn_context.getBID();
        operation_chain_construction_modify_read(t_record, srcTable, bid, accessType, record_ref, function, txn_context);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;
    }


    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord s_record, SchemaRecordRef record_ref, Function function,
                                              TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {

        long bid = txn_context.getBID();
        operation_chain_construction_modify_read(srcTable, bid, accessType,
                s_record, record_ref, function, condition_source, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;

    }

    @Override
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord s_record, TableRecord d_record, Function function, TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {

        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(srcTable, bid, accessType, s_record, d_record, function, condition_source, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;
    }

    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord s_record, Function function, TableRecord[] condition_source, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {

        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(srcTable, bid, accessType, s_record, function, condition_source, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.

        return true;
    }

    /**
     * This is the API: SP-Layer inform the arrival of checkpoint, which informs the TP-Layer to start evaluation.
     *
     * @param thread_Id
     * @param mark_ID
     * @return time spend in tp evaluation.
     */
    @Override
    public void start_evaluate(int thread_Id, long mark_ID) throws InterruptedException, BrokenBarrierException {

        instance.start_evaluation(thread_Id, mark_ID);
    }
}
