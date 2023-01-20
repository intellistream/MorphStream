package transaction;

import content.common.CommonMetaTypes;
import db.DatabaseException;
import lock.OrderLock;
import lock.PartitionedOrderLock;
import scheduler.context.SchedulerContext;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

/**
 * Every thread has its own TxnManager.
 */
public interface ITxnManager {

    OrderLock getOrderLock();//shared.

    PartitionedOrderLock.LOCK getOrderLock(int p_id);//partitioned. Global ordering can not be partitioned.

    /**
     * Read-only
     * This API pushes a place-holder to the shared-store.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param record_ref   expect a return value_list from the store to support further computation in the application.
     * @param enqueue_time
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, double[] enqueue_time, String operator_name) throws DatabaseException;
    //Used by native T-Stream.

    /**
     * Read-only
     * This API pushes a place-holder to the shared-store.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param record_ref   expect a return value_list from the store to support further computation in the application.
     * @param enqueue_time
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String key, TableRecordRef record_ref, double[] enqueue_time, String operator_name) throws DatabaseException;

    /**
     * Write-only
     * <p>
     * This API installes the given value_list to specific d_record and return.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param value
     * @param enqueue_time
     * @return
     * @throws DatabaseException
     */
    boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String key, List<DataBox> value, double[] enqueue_time, String operator_name) throws DatabaseException;

    boolean Asy_WriteRecord(TxnContext txn_context, String table, String id, long value, int column_id, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, Condition condition, int[] success, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, int[] success, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, int[] success, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord_Iteration(TxnContext txn_context, String srcTable, String key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, int[] success, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord_Iteration_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, int[] success, String operator_name, boolean doUpdate) throws DatabaseException;

    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, int[] success, String operator_name) throws DatabaseException;

    boolean Asy_ModifyRecord_ReadN(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, int[] success, String operator_name) throws DatabaseException;

    //used by speculative T-Stream.
//    boolean Specu_ReadRecord(TxnContext txn_context, String microTable, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException;
    void start_evaluate(int taskId, double mark_ID, int num_events) throws InterruptedException, BrokenBarrierException;

    boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    void InsertNewRecord(String table_name, String key, SchemaRecord record)  throws DatabaseException;

    boolean SelectKeyRecord(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException;

    boolean lock_ahead(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    void BeginTransaction(TxnContext txnContext);

    boolean CommitTransaction(TxnContext txn_context);

    SchedulerContext getSchedulerContext();

    enum SCHEDULER_TYPE {
        OG_BFS,
        OG_BFS_A,
        OG_DFS,
        OG_DFS_A,
        OG_NS,
        OG_NS_A,
        OP_NS,
        OP_NS_A,
        OP_BFS,
        OP_BFS_A,
        OP_DFS,
        OP_DFS_A,
        TStream // original tstream
        }
}
