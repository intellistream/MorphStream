package transaction;

import common.OrderLock;
import common.PartitionedOrderLock;
import common.meta.MetaTypes;
import db.Database;
import db.DatabaseException;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.SchemaRecords;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

public interface TxnManager {
    OrderLock getOrderLock();//shared.

    PartitionedOrderLock.LOCK getOrderLock(int p_id);//partitioned. Global ordering can not be partitioned.
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
    boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException;

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
    boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String key, TableRecordRef record_ref, double[] enqueue_time) throws DatabaseException;

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
    boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String key, List<DataBox> value, double[] enqueue_time) throws DatabaseException;

    boolean Asy_WriteRecord(TxnContext txn_context, String table, String id, long value, int column_id) throws DatabaseException;
    /**
     * @param txn_context
     * @param srcTable
     * @param key
     * @param value_list
     * @param columnId
     * @param enqueue_time
     */
//    boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String key, long value_list, int columnId, double[] enqueue_time) throws DatabaseException;

    /**
     * Read-Modify_Write without condition checking... the event can be immediately emitted.
     *
     * @param txn_context
     * @param srcTable
     * @param source_key
     * @param dest_key
     * @param function    the pushdown function.
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, String dest_key, Function function) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException;

    /**
     * Read-Modify_Write w/ read.
     *
     * @param txn_context
     * @param srcTable
     * @param source_key
     * @param dest_key
     * @param record_ref  expect a return value_list from the store to support further computation in the application.
     * @param function    the pushdown function.
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String source_key, String dest_key, SchemaRecordRef record_ref, Function function) throws DatabaseException;

    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException;

    /**
     * Multi-keys reading with condition checking.
     *
     * @param txn_context
     * @param srcTable
     * @param src_key
     * @param dest_key
     * @param function
     * @param condition_sourceTable
     * @param condition_source
     * @param success
     * @return
     * @throws DatabaseException
     */
    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String src_key, String dest_key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, boolean[] success) throws DatabaseException;

    boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException;

    boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException;

    //used by speculative T-Stream.
//    boolean Specu_ReadRecord(TxnContext txn_context, String microTable, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException;
    void start_evaluate(int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException;

    boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    boolean CommitTransaction(TxnContext txn_context);

    boolean SelectKeyRecord(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException, InterruptedException;

    boolean SelectRecords(Database db, TxnContext txn_context, String table_name, int i, String secondary_key, SchemaRecords records, MetaTypes.AccessType accessType, LinkedList<Long> gap);

    boolean lock_ahead(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException;

    boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException;
}
