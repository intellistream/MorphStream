package state_engine.transaction.dedicated.ordered;
import common.collections.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.Meta.MetaTypes;
import state_engine.common.Operation;
import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.storage.*;
import state_engine.storage.datatype.DataBox;
import state_engine.transaction.dedicated.TxnManagerDedicated;
import state_engine.transaction.function.Condition;
import state_engine.transaction.function.Function;
import state_engine.transaction.impl.TxnContext;
import state_engine.transaction.scheduler.BaseLineScheduler;
import state_engine.utils.SOURCE_CONTROL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
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
    public TxnManagerTStream(StorageManager storageManager, String thisComponentId, int thisTaskId, int numberOfStates, int thread_countw) {
        super(storageManager, thisComponentId, thisTaskId, thread_countw);
        instance = TxnProcessingEngine.getInstance();
//        delta_long = (int) Math.ceil(NUM_ITEMS / (double) thread_countw);//range of each partition. depends on the number of op in the stage.
//        delta = (int) Math.ceil(NUM_ACCOUNTS / (double) thread_countw);//NUM_ITEMS / tthread;
        delta = (int) Math.ceil( numberOfStates / (double) thread_countw ); // Check id generation in DateGenerator.
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
        //DD: Number of accounts / threads (tasks) gives us delta and record key is probably incremental upto number of accounts.
//        System.out.println("Thread id: "+(_key / delta));
        return _key / delta;
//        return _key % 12;
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
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        MyList<Operation> myList = holder.get(primaryKey);
//        LOG.info(String.valueOf(OsUtils.Addresser.addressOf(record_ref)));
//        myList.add(new Operation(table_name, txn_context, bid, accessType, record, record_ref));
        addOperationToChain(new Operation(table_name, txn_context, bid, accessType, record, record_ref), table_name, primaryKey);
//        Integer key = Integer.valueOf(record.record_.GetPrimaryKey());
//        int taskId = getTaskId(key);
//        int h2ID = getH2ID(key);
//        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
//        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, record_ref));
    }
    public void operation_chain_construction_read_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, TableRecordRef record_ref, TxnContext txn_context) {
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        MyList<Operation> myList = holder.get(primaryKey);
//        LOG.info(String.valueOf(OsUtils.Addresser.addressOf(record_ref)));
//        myList.add(new Operation(table_name, txn_context, bid, accessType, record, record_ref));
        addOperationToChain(new Operation(table_name, txn_context, bid, accessType, record, record_ref), table_name, primaryKey);
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

//        ConcurrentHashMap<String, MyList<Operation>> holder =
//                // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
//                instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, value));

        addOperationToChain(new Operation(table_name, txn_context, bid, accessType, record, value), table_name, primaryKey);
//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, value_list));
    }
    private void operation_chain_construction_write_only(TableRecord record, String primaryKey, String table_name, long bid, MetaTypes.AccessType accessType, long value, int column_id, TxnContext txn_context) {
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, value, column_id));
        addOperationToChain(new Operation(table_name, txn_context, bid, accessType, record, value, column_id), table_name, primaryKey);
//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, value_list));
    }
    private void operation_chain_construction_modify_read(TableRecord record, String table_name, long bid,
                                                          MetaTypes.AccessType accessType, SchemaRecordRef record_ref, Function function, TxnContext txn_context) {
//        String primaryKey = record.record_.GetPrimaryKey();
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        //simple sequential build.
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, txn_context, bid, accessType, record, record_ref, function));
        addOperationToChain(new Operation(table_name, txn_context, bid, accessType, record, record_ref, function), table_name, record.record_.GetPrimaryKey());
//        int taskId = getTaskId(record);
//        int h2ID = getH2ID(taskId);
//        LOG.debug("Submit read for record:" + record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(record)).holder_v3;
//        holder.add(new Operation(txn_context, bid, accessType, record, record_ref, function));
    }

    //READ_WRITE
    private void operation_chain_construction_modify_only(TableRecord s_record, String table_name, long bid, MetaTypes.AccessType accessType, TableRecord d_record, Function function, TxnContext txn_context, int column_id) {
//        String primaryKey = d_record.record_.GetPrimaryKey();
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, s_record, d_record, bid, accessType, function, txn_context, column_id));
        addOperationToChain(new Operation(table_name, s_record, d_record, bid, accessType, function, txn_context, column_id), table_name, d_record.record_.GetPrimaryKey());
//        updateDependencies();
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(s_record)).holder_v3;
//        holder.add(new Operation(s_record, d_record, bid, accessType, function, txn_context));
    }
    private void operation_chain_construction_modify_only(String table_name, String key, long bid, MetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, Function function,
                                                          String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
//        String primaryKey = d_record.record_.GetPrimaryKey();
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success));
        addOperationToChain(new Operation(table_name, s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success), table_name, d_record.record_.GetPrimaryKey());

//
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(table_name).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(s_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success));
    }

    //READ_WRITE_COND // TRANSFER_AST
    private void operation_chain_construction_modify_only(String table_name, String key, long bid, MetaTypes.AccessType accessType, TableRecord d_record, Function function,
                                                          String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
//        String primaryKey = d_record.record_.GetPrimaryKey();
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, d_record, bid, accessType, function, condition_records, condition, txn_context, success));
        MeasureTools.BEGIN_CREATE_OC_TIME_MEASURE(txn_context.thread_Id);
        Operation op = new Operation(table_name, d_record, bid, accessType, function, condition_records, condition, txn_context, success);
        addOperationToChain(op, table_name, d_record.record_.GetPrimaryKey());
        MeasureTools.END_CREATE_OC_TIME_MEASURE(txn_context.thread_Id);

        MeasureTools.BEGIN_DEPENDENCY_CHECKING_TIME_MEASURE(txn_context.thread_Id);
        checkDataDependencies(op, txn_context.thread_Id, table_name, key, condition_sourceTable, condition_source);
        MeasureTools.END_DEPENDENCY_CHECKING_TIME_MEASURE(txn_context.thread_Id);
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
////        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(fid).rangeMap.get(taskId).holder_v2[h2ID];
////        Set<Operation> holder = instance.getHolder(fid).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(d_record, bid, accessType, function, condition_records, condition, txn_context, success));
    }

    //READ_WRITE_COND_READ // TRANSFER_ACT
    private void operation_chain_construction_modify_read(String table_name, String key, long bid, MetaTypes.AccessType accessType, TableRecord d_record, SchemaRecordRef record_ref, Function function,
                                                          String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, TxnContext txn_context, boolean[] success) {
//        String primaryKey = d_record.record_.GetPrimaryKey();
//        ConcurrentHashMap<String, MyList<Operation>> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        holder.get(primaryKey).add(new Operation(table_name, d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success));
        MeasureTools.BEGIN_CREATE_OC_TIME_MEASURE(txn_context.thread_Id);
        Operation op = new Operation(table_name, d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success);
        addOperationToChain(op, table_name, d_record.record_.GetPrimaryKey());
        MeasureTools.END_CREATE_OC_TIME_MEASURE(txn_context.thread_Id);

        MeasureTools.BEGIN_DEPENDENCY_CHECKING_TIME_MEASURE(txn_context.thread_Id);
        checkDataDependencies(op, txn_context.thread_Id, table_name, key, condition_sourceTable, condition_source);
        MeasureTools.END_DEPENDENCY_CHECKING_TIME_MEASURE(txn_context.thread_Id);
//        int taskId = getTaskId(d_record);
//        int h2ID = getH2ID(taskId);
//        LOG.debug("Submit read for record:" + d_record.record_.GetPrimaryKey() + " in H2ID:" + h2ID);
//        MyList<Operation> holder = instance.getHolder(fid).rangeMap.get(taskId).holder_v2[h2ID];
//        Set<Operation> holder = instance.getHolder(fid).rangeMap.get(getTaskId(d_record)).holder_v3;
//        holder.add(new Operation(d_record, d_record, record_ref, bid, accessType, function, condition_records, condition, txn_context, success));
    }

    private void checkDataDependencies(Operation op, int thread_Id, String table_name, String key, String[] condition_sourceTable, String[] condition_source) {

        OperationChain dependent = instance.getHolder(table_name).rangeMap.get(getTaskId(key)).holder_v1.get(key);

        for (int index=0; index<condition_source.length; index++) {
            if(table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;

            ConcurrentHashMap<String, OperationChain> dependencyHolder = instance.getHolder(condition_sourceTable[index]).rangeMap.get(getTaskId(condition_source[index])).holder_v1;
            OperationChain dependency = dependencyHolder.get(condition_source[index]);
            if(dependency == null) {
                // we add dependency operation chain for the sake of recording a potential dependency on it due to some delayed event arrival.
                dependency = new OperationChain(condition_sourceTable[index], condition_source[index]);
                dependencyHolder.put(condition_source[index], dependency);
            }
//            System.out.println("Checking dependencies...");
            // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is  enough.
            if(dependency.getOperations().isEmpty() || dependency.getOperations().first().bid >= op.bid) { // if dependencies first op's bid is >= current bid, then it has no operation that we depend upon, but it could be a potential dependency in case we have delayed transactions (events)
                // if dependency has no operations on it or no operation with id < current operation id.
                // we will like to record it as potential future dependency, if a delayed operation with id < current bid arrives
                dependency.addPotentialDependent(dependent, op);
            } else { // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                dependent.addDependency(op, dependency); // record dependency
            }
//            System.out.println("Checking dependencies...done");
        }
        // check if current operation causes other operation chains to depend upon current one.
        // This may happen when a delayed event arrives.

//        System.out.println("Checking other dependencies...");
        MeasureTools.BEGIN_DEPENDENCY_OUTOFORDER_OVERHEAD_TIME_MEASURE(thread_Id);
        dependent.checkOtherPotentialDependencies(op);
        MeasureTools.END_DEPENDENCY_OUTOFORDER_OVERHEAD_TIME_MEASURE(thread_Id);
//        System.out.println("Checking other dependencies...done");

    }

    private void addOperationToChain(Operation operation, String table_name, String primaryKey){
        // DD: Get the Holder for the table, then get a map for each thread, then get the list of operations
        ConcurrentHashMap<String, OperationChain> holder = instance.getHolder(table_name).rangeMap.get(getTaskId(primaryKey)).holder_v1;
        holder.putIfAbsent(primaryKey, new OperationChain(table_name, primaryKey));
        holder.get(primaryKey).addOperation(operation);

//        holder.putIfAbsent(primaryKey, new MyList(table_name, primaryKey));
//        MyList<Operation> myList = holder.get(primaryKey);
//        myList.add(operation);
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

    // TRANSFER_ACT
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, String key, TableRecord s_record, SchemaRecordRef record_ref, Function function,
                                              String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_read(srcTable, key, bid, accessType,
                s_record, record_ref, function, condition_sourceTable, condition_source, condition_records, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.
        return true;
    }
    @Override
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, String key, TableRecord s_record, TableRecord d_record, Function function,
                                         String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(srcTable, key, bid, accessType, s_record, d_record, function, condition_sourceTable, condition_source, condition_records, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.
        return true;
    }
    // TRANSFER_AST
    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, String key, TableRecord s_record, Function function,
                                         String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, MetaTypes.AccessType accessType, boolean[] success) {
        long bid = txn_context.getBID();
        operation_chain_construction_modify_only(srcTable, key, bid, accessType, s_record, function, condition_sourceTable, condition_source, condition_records, condition, txn_context, success);//TODO: this is for sure READ_WRITE... think about how to further optimize.
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

        MeasureTools.BEGIN_BARRIER_TIME_MEASURE(thread_Id);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        MeasureTools.END_BARRIER_TIME_MEASURE(thread_Id);

        MeasureTools.BEGIN_SUBMIT_TOTAL_TIME_MEASURE(thread_Id);
        ArrayList<OperationChain> ocs = new ArrayList<>();
        Collection<TxnProcessingEngine.Holder_in_range> tablesHolderInRange = instance.getHolder().values();
        for (TxnProcessingEngine.Holder_in_range tableHolderInRange : tablesHolderInRange) {
            ocs.addAll(tableHolderInRange.rangeMap.get(thread_Id).holder_v1.values());
        }
        instance.getScheduler().submitOcs(thread_Id, ocs);
        MeasureTools.END_SUBMIT_TOTAL_TIME_MEASURE(thread_Id);

        MeasureTools.BEGIN_BARRIER_TIME_MEASURE(thread_Id);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        MeasureTools.END_BARRIER_TIME_MEASURE(thread_Id);

        instance.start_evaluation(thread_Id, mark_ID);

        for (TxnProcessingEngine.Holder_in_range tableHolderInRange : tablesHolderInRange) {
            tableHolderInRange.rangeMap.get(thread_Id).holder_v1.clear();
        }

        MeasureTools.BEGIN_BARRIER_TIME_MEASURE(thread_Id);
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
        MeasureTools.END_BARRIER_TIME_MEASURE(thread_Id);

    }

    public void dumpDependenciesForThread(int thread_id) { // SL Specific code in TxnManager, where else to put it?

        ArrayList<String> dependencies = new ArrayList<>();
        ConcurrentHashMap<String, OperationChain> accountsHolder = instance.getHolder("accounts").rangeMap.get(thread_id).holder_v1;
        ConcurrentHashMap<String, OperationChain> booksHolder = instance.getHolder("bookEntries").rangeMap.get(thread_id).holder_v1;

        ConcurrentHashMap.KeySetView<String, OperationChain> keys = accountsHolder.keySet();
        for (String key : keys) {
            accountsHolder.get(key).addAllDependencies(dependencies);
        }
        keys = booksHolder.keySet();
        for (String key : keys) {
            booksHolder.get(key).addAllDependencies(dependencies);
        }

        FileWriter fileWriter = null;
        try {

            File file = new File(System.getProperty("user.home") + OsUtils.OS_wrapper("sesame") + OsUtils.OS_wrapper("SYNTH_DATA") + OsUtils.OS_wrapper(String.format("dependency_edges_thread_%d.csv", thread_id)));
            if (file.exists())
                file.delete();
            file.createNewFile();

            fileWriter = new FileWriter(file);
            fileWriter.write("source,target\n");

            for(String dependency: dependencies)
                fileWriter.write(dependency+"\n");
            fileWriter.close();
            System.out.println("Recorded dependencies dumped...thread: "+thread_id);
        } catch (IOException e) {
            System.out.println("An error occurred while storing dependencies graph.");
            e.printStackTrace();
        }

    }


    private void mergeDependencyFiles() {

        ArrayList<String> dependencies = new ArrayList<>();

        for(int lop=0; lop<thread_count_; lop++) {

        }
    }

}
