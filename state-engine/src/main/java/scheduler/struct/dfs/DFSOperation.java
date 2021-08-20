package scheduler.struct.dfs;

import content.common.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
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
public class DFSOperation extends AbstractOperation implements Comparable<DFSOperation> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
    public final SchedulerContext context;
    private final String operationChainKey;

    private final Queue<DFSOperation> fd_children; // the functional dependencies ops to be executed after this op.
    private final Queue<DFSOperation> fd_parents; // the functional dependencies ops to be executed in advance
    private final AtomicReference<OperationStateType> operationState;
    // operation id under a transaction.
    // an operation id to indicate how many operations in front of this operation in the same transaction.
    public int txn_op_id = 0;
    public boolean isFailed;
    public String name;

    private DFSOperationChain oc; // used for dependency resolved notification under greedy smart

    public DFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public DFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public DFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends SchedulerContext> DFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends SchedulerContext> DFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends SchedulerContext> DFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                           SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends SchedulerContext> DFSOperation(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid);
        this.context = context;
        this.operationChainKey = table_name + "|" + d_record.record_.GetPrimaryKey();

        // finctional dependencies
        fd_parents = new ConcurrentLinkedQueue<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ConcurrentLinkedQueue<>(); // the finctional dependencies ops to be executed after this op.
        // temporal dependencies
        operationState = new AtomicReference<>(OperationStateType.BLOCKED);
    }


    /****************************End*************************************/


    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    @Override
    public int compareTo(DFSOperation operation) {
        if (this.bid == (operation.bid)) {
            if (this.d_record.getID() - operation.d_record.getID() == 0) {
                return this.getTxn_op_id() - operation.getTxn_op_id();
            }
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    @Override
    public String toString() {
        return bid + "|" + txn_op_id + "|" + String.format("%-15s", this.getOperationState());
    }

    public void set_op_id(int op_id) {
        this.txn_op_id = op_id;
    }

    public int getTxn_op_id() {
        return txn_op_id;
    }

    /*********************************CREATED BY MYC****************************************/


    public String getOperationChainKey() {
        return operationChainKey;
    }

    public DFSOperationChain getOC() {
        if (oc == null) {
            throw new RuntimeException("the returned oc cannot be null");
        }
        return oc;
    }

    public void setOC(DFSOperationChain operationChain) {
        this.oc = operationChain;
    }

    public <T extends AbstractOperation> Queue<T> getChildren(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return (Queue<T>) fd_children;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }


    public void addParent(DFSOperation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_parents.add(operation);
            // get the operation chain and update the ld dependencies
//            this.getOC().addParentOrChild(operation.getOC(), MetaTypes.DependencyType.FD, false);
        } else {
            throw new RuntimeException("unsupported dependency type parent");
        }
    }

    public void addChild(DFSOperation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_children.add(operation);
//            this.getOC().addParentOrChild(operation.getOC(), DependencyType.FD, true);
        } else {
            throw new RuntimeException("unsupported dependency type children");
        }
    }

    public void stateTransition(OperationStateType state) {
        LOG.debug(this + " : state transit " + operationState + " -> " + state);
        operationState.getAndSet(state);
    }

    public OperationStateType getOperationState() {
        return operationState.get();
    }


    /**
     * @param type
     * @param <T>
     * @return
     */
    public <T extends AbstractOperation> Queue<T> getParents(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return (Queue<T>) fd_parents;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }
}