package scheduler.struct.gs;

import content.common.CommonMetaTypes;
import scheduler.context.AbstractGSTPGContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.MetaTypes.OperationStateType;
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
public class GSOperation extends AbstractOperation implements Comparable<GSOperation> {
    public final AbstractGSTPGContext context;

    private final Queue<GSOperation> fd_children; // the functional dependencies ops to be executed after this op.
    private final Queue<GSOperation> fd_parents; // the functional dependencies ops to be executed in advance

    public GSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public GSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                       Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public GSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                       SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends AbstractGSTPGContext> GSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends AbstractGSTPGContext> GSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends AbstractGSTPGContext> GSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                              SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends AbstractGSTPGContext> GSOperation(
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
        AtomicReference<OperationStateType> operationState = new AtomicReference<>(OperationStateType.BLOCKED);
    }

    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    @Override
    public int compareTo(GSOperation operation) {
        if (this.bid == (operation.bid)) {
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    @Override
    public String toString() {
        return String.valueOf(bid);
    }

    /*********************************Dependencies setup****************************************/

    public void addChild(GSOperation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_children.add(operation);
        } else {
            throw new RuntimeException("unsupported dependency type children");
        }
    }


    public Queue<GSOperation> getChildren(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return fd_children;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }

    /**
     * @param type
     * @return
     */
    public Queue<GSOperation> getParents(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return fd_parents;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }
}