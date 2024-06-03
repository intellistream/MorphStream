package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.record.SchemaRecordRef;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

/**
 * Every thread has its own TxnManager.
 */
public interface ITxnManager {

    //used by speculative T-Stream.
    void start_evaluate(String operatorID, int batchID, int num_events, int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException;
    boolean submitStateAccess(Function function, FunctionContext functionContext) throws DatabaseException;

    boolean InsertRecord(FunctionContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    void BeginTransaction(FunctionContext functionContext);

    boolean CommitTransaction(FunctionContext txn_context, int batchID);
    void switch_scheduler(int thread_Id, long mark_ID, int batchID, String operatorID);
    boolean SelectKeyRecord(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException;

    boolean lock_ahead(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    boolean SelectKeyRecord_noLock(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException;

    boolean lock_all(SpinLock[] spinLocks) throws DatabaseException;

    boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException;
    OrderLock getOrderLock();//shared.

    PartitionedOrderLock.LOCK getOrderLock(int p_id);//partitioned. Global ordering can not be partitioned.

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
        TStream, // original TStream
        Recovery,
        DScheduler,
        RLScheduler,
        OCCScheduler
    }

}
