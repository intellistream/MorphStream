package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

/**
 * Every thread has its own TxnManager.
 */
public interface ITxnManager {

    OrderLock getOrderLock();//shared.

    PartitionedOrderLock.LOCK getOrderLock(int p_id);//partitioned. Global ordering can not be partitioned.
    boolean submitStateAccess(StateAccess stateAccess, TxnContext txnContext) throws DatabaseException;

    //used by speculative T-Stream.
//    boolean Specu_ReadRecord(TxnContext txn_context, String microTable, String key, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) throws DatabaseException;
    void start_evaluate(int taskId, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException;

    boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    boolean SelectKeyRecord(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException;

    boolean lock_ahead(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    boolean lock_all(SpinLock[] spinLocks) throws DatabaseException;

    boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException;

    void BeginTransaction(TxnContext txnContext);

    boolean CommitTransaction(TxnContext txn_context);

    SchedulerContext getSchedulerContext();

    void switch_scheduler(int thread_Id, long mark_ID);

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
        TStream, // original TStream
        Recovery
    }

}
