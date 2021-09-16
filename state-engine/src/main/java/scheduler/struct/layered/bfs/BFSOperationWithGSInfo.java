package scheduler.struct.layered.bfs;

import content.common.CommonMetaTypes;
import scheduler.context.SchedulerContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes;
import scheduler.struct.gs.GSOperation;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class BFSOperationWithGSInfo extends AbstractOperation {

    public final SchedulerContext context;

    private final Queue<GSOperation> fd_children; // the functional dependencies ops to be executed after this op.
    private final Queue<GSOperation> fd_parents; // the functional dependencies ops to be executed in advance

    public BFSOperationWithGSInfo(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public BFSOperationWithGSInfo(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                                  Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public BFSOperationWithGSInfo(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                                  SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends SchedulerContext> BFSOperationWithGSInfo(Context context, String table_name, TxnContext txn_context, long bid,
                                                                     CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends SchedulerContext> BFSOperationWithGSInfo(Context context, String table_name, TxnContext txn_context, long bid,
                                                                     CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends SchedulerContext> BFSOperationWithGSInfo(Context context, String table_name, TxnContext txn_context, long bid,
                                                                     CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                                     SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends SchedulerContext> BFSOperationWithGSInfo(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid);

        this.context = context;

        // finctional dependencies
        fd_parents = new ConcurrentLinkedQueue<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ConcurrentLinkedQueue<>(); // the finctional dependencies ops to be executed after this op.
//        ld_descendant_operations = new ConcurrentLinkedQueue<>();
        // temporal dependencies
        AtomicReference<MetaTypes.OperationStateType> operationState = new AtomicReference<>(MetaTypes.OperationStateType.BLOCKED);
    }
}