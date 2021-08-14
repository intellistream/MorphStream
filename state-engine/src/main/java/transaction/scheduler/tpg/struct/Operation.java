package transaction.scheduler.tpg.struct;

import common.meta.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnContext;
import transaction.scheduler.common.AbstractOperation;
import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.MetaTypes.OperationStateType;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class Operation extends AbstractOperation implements Comparable<Operation> {
    public final static int NONE_CANDIDATE = 0;
    public final static int SPECULATIVE_CANDIDATE = 1;
    public final static int READY_CANDIDATE = 2;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
    public final TPGContext context;
    private final String operationChainKey;

    private final Queue<Operation> ld_descendant_operations;
    private final Queue<Operation> fd_children; // the functional dependencies ops to be executed after this op.
    private final Queue<Operation> td_children; // the functional dependencies ops to be executed after this op.
    private final Queue<Operation> ld_children; // the functional dependencies ops to be executed after this op.
    private final Queue<Operation> ld_spec_children; // speculative children to notify.
    private final Queue<Operation> fd_parents; // the functional dependencies ops to be executed in advance
    private final Queue<Operation> td_parents; // the functional dependencies ops to be executed in advance
    private final Queue<Operation> ld_parents; // the functional dependencies ops to be executed in advance
    private final Queue<Operation> ld_spec_parents; // speculative parents to wait, include the last ready op
    private final OperationMetadata operationMetadata;
    private final AtomicReference<OperationStateType> operationState;
    // operation id under a transaction.
    // an operation id to indicate how many operations in front of this operation in the same transaction.
    public int txn_op_id = 0;
    public boolean isFailed;
    public String name;

    private OperationChain oc; // used for dependency resolved notification under greedy smart
    private AbstractOperation ld_head_operation;

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends TPGContext> Operation(Context context, String table_name, TxnContext txn_context, long bid,
                                                  CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends TPGContext> Operation(Context context, String table_name, TxnContext txn_context, long bid,
                                                  CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends TPGContext> Operation(Context context, String table_name, TxnContext txn_context, long bid,
                                                  CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                  SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends TPGContext> Operation(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid);
        this.context = context;
        this.operationChainKey = table_name + "|" + d_record.record_.GetPrimaryKey();

        ld_head_operation = null;
        ld_descendant_operations = new ArrayDeque<>();

        // finctional dependencies
        fd_parents = new ArrayDeque<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ArrayDeque<>(); // the finctional dependencies ops to be executed after this op.
        // temporal dependencies
        td_parents = new ArrayDeque<>(); // the finctional dependnecies ops to be executed in advance
        td_children = new ArrayDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        ld_parents = new ArrayDeque<>(); // the finctional dependnecies ops to be executed in advance
        ld_children = new ArrayDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        ld_spec_parents = new ArrayDeque<>(); // speculative parents to wait, include the last ready op
        ld_spec_children = new ArrayDeque<>(); // speculative children to notify.

        operationMetadata = new Operation.OperationMetadata();
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
    public int compareTo(Operation operation) {
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

    public void setOC(OperationChain operationChain) {
        this.oc = operationChain;
    }

    public OperationChain getOC() {
        if (oc == null) {
            throw new RuntimeException("the returned oc cannot be null");
        }
        return oc;
    }

    public <T extends AbstractOperation> ArrayDeque<T> getChildren(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return (ArrayDeque<T>) fd_children;
        } else if (type.equals(DependencyType.TD)) {
            return (ArrayDeque<T>) td_children;
        } else if (type.equals(DependencyType.LD)) {
            return (ArrayDeque<T>) ld_children;
        } else if (type.equals(DependencyType.SP_LD)) {
            return (ArrayDeque<T>) ld_spec_children;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }

    public boolean isRoot() {
        return operationMetadata.td_countdown[0].get() == 0 && operationMetadata.fd_countdown[0].get() == 0 && operationMetadata.ld_spec_countdown[0].get() == 0;
    }

    public void addParent(Operation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_parents.add(operation);
            this.operationMetadata.fd_countdown[0].incrementAndGet();
            // get the operation chain and update the ld dependencies
            this.getOC().addParentOrChild(operation.getOC(), MetaTypes.DependencyType.FD, false);
        } else if (type.equals(DependencyType.LD)) {
            this.ld_parents.add(operation);
            this.operationMetadata.ld_countdown[0].incrementAndGet();
//            this.getOC().addParentOrChild(operation.getOC(), MetaTypes.DependencyType.LD, false);
        } else if (type.equals(DependencyType.SP_LD)) {
            this.ld_spec_parents.add(operation);
            this.operationMetadata.ld_spec_countdown[0].incrementAndGet();
        } else if (type.equals(DependencyType.TD)) {
            this.td_parents.add(operation);
            this.operationMetadata.td_countdown[0].incrementAndGet();
            this.operationMetadata.td_countdown[1].incrementAndGet();
        } else {
            throw new RuntimeException("unsupported dependency type parent");
        }
    }

    public void addChild(Operation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_children.add(operation);
            this.getOC().addParentOrChild(operation.getOC(), DependencyType.FD, true);
        } else if (type.equals(DependencyType.LD)) {
            this.ld_children.clear();
            this.ld_children.add(operation);
//            this.getOC().addParentOrChild(operation.getOC(), DependencyType.LD, true);
        } else if (type.equals(DependencyType.SP_LD)) {
            this.ld_spec_children.add(operation);
        } else if (type.equals(DependencyType.TD)) {
            this.td_children.add(operation);
        } else {
            throw new RuntimeException("unsupported dependency type children");
        }
    }

    public void addHeader(Operation header) {
        ld_head_operation = header;
    }

    public void addDescendant(Operation descendant) {
        ld_descendant_operations.add(descendant);
        operationMetadata.ld_descendant_countdown.incrementAndGet();
        assert ld_descendant_operations.size() == operationMetadata.ld_descendant_countdown.get();
    }

    public boolean isReadyCandidate() {
        return operationMetadata.is_spec_or_ready_candidate.get() == READY_CANDIDATE;
    }

    public boolean isSpeculativeCandidate() {
        return operationMetadata.is_spec_or_ready_candidate.get() == SPECULATIVE_CANDIDATE;
    }

    public void stateTransition(OperationStateType state) {
        LOG.debug(this + " : state transit " + operationState + " -> " + state);
        operationState.getAndSet(state);
    }

    public OperationStateType getOperationState() {
        return operationState.get();
    }

    // promote header to be the first ready candidate of the transaction
    public void setReadyCandidate() {
        operationMetadata.is_spec_or_ready_candidate.getAndSet(READY_CANDIDATE);
    }

    public void setSpeculativeCandidate() {
        operationMetadata.is_spec_or_ready_candidate.compareAndSet(NONE_CANDIDATE, SPECULATIVE_CANDIDATE);
    }

    /**
     * Modify CountDown Variables.
     *
     * @param dependencyType
     * @param parentState
     */
    public void updateDependencies(DependencyType dependencyType, OperationStateType parentState) {
        // update countdown
        if (parentState.equals(OperationStateType.EXECUTED)) {
            if (dependencyType.equals(DependencyType.TD)) {
                operationMetadata.td_countdown[0].decrementAndGet();
                assert operationMetadata.td_countdown[0].get() >= 0;
            } else if (dependencyType.equals(DependencyType.FD)) {
                operationMetadata.fd_countdown[0].decrementAndGet();
                assert operationMetadata.fd_countdown[0].get() >= 0;
            } else if (dependencyType.equals(DependencyType.SP_LD)) {
                operationMetadata.ld_spec_countdown[0].decrementAndGet();
            }
        } else if (parentState.equals(OperationStateType.COMMITTED)) {
            if (dependencyType.equals(DependencyType.TD)) {
                operationMetadata.td_countdown[1].decrementAndGet();
                assert operationMetadata.td_countdown[1].get() >= 0;
            }
        }
    }

    public void updateCommitCountdown() {
        // update countdown
        operationMetadata.ld_descendant_countdown.decrementAndGet();
    }

    public boolean trySpeculative(boolean isRollback) {
        boolean isSpeculative = operationMetadata.td_countdown[0].get() == 0
                && operationMetadata.fd_countdown[0].get() == 0
                && operationMetadata.is_spec_or_ready_candidate.get() == SPECULATIVE_CANDIDATE;
        if (isRollback)
            isSpeculative = isSpeculative
                    && this.getOperationState().equals(OperationStateType.EXECUTED);
        else
            isSpeculative = isSpeculative &&
                    this.getOperationState().equals(OperationStateType.BLOCKED);
        if (isSpeculative) {
            stateTransition(OperationStateType.SPECULATIVE);
        }
        return isSpeculative;
    }

    public boolean tryReady(boolean isRollback) {
        boolean isReady = operationMetadata.td_countdown[0].get() == 0
                && operationMetadata.fd_countdown[0].get() == 0
                && operationMetadata.ld_spec_countdown[0].get() == 0
                && operationMetadata.is_spec_or_ready_candidate.get() == READY_CANDIDATE;
        if (isRollback)
            isReady = isReady
                    && this.getOperationState().equals(OperationStateType.EXECUTED);
        else
            isReady = isReady
                    && (this.getOperationState().equals(OperationStateType.BLOCKED));
        if (isReady) {
            stateTransition(OperationStateType.READY);
        }
        return isReady;
    }

    public boolean tryHeaderCommit() {
        // header check whether all descendants are in committable, if so, commit.
        if (!this.equals(ld_head_operation)) {
            throw new RuntimeException("this can be only invoked by header.");
        }
        LOG.debug("++++++" + this + " check whether committable: " + ld_descendant_operations);
        boolean readyToCommit = this.getOperationState().equals(OperationStateType.COMMITTABLE);
        readyToCommit = readyToCommit && operationMetadata.ld_descendant_countdown.get() == 0;
        if (readyToCommit) {
            // notify all descendants to commit.
            stateTransition(OperationStateType.COMMITTED);
        }
        return readyToCommit;
    }

    public boolean tryCommittable() {
        LOG.debug("++++++" + this + " check committable:");
        boolean isCommittable =
                operationMetadata.td_countdown[1].get() == 0
                        && this.getOperationState().equals(OperationStateType.EXECUTED);
        LOG.debug("++++++" + this + " try commit results: " +
                (operationMetadata.td_countdown[1].get() == 0)
                + " | " + this.getOperationState().equals(OperationStateType.EXECUTED));
        if (isCommittable) {
            // EXECUTED->COMMITTABLE
            stateTransition(OperationStateType.COMMITTABLE);
        } else {
            LOG.debug("++++++" + this + " committable failed");
        }
        LOG.debug("++++++" + this + " check committable end");
        return isCommittable;
    }

    /**
     * @param type
     * @param <T>
     * @return
     */
    public <T extends AbstractOperation> Queue<T> getParents(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return (Queue<T>) fd_parents;
        } else if (type.equals(DependencyType.TD)) {
            return (Queue<T>) td_parents;
        } else if (type.equals(DependencyType.LD)) {
            return (Queue<T>) ld_parents;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }

    public <T extends AbstractOperation> T getHeader() {
        return (T) ld_head_operation;
    }

    public <T extends AbstractOperation> ArrayDeque<T> getDescendants() {
        return (ArrayDeque<T>) ld_descendant_operations;
    }

    public boolean isHeader() {
        return this.equals(ld_head_operation);
    }

    private static class OperationMetadata {
        // countdown to be executed parents and committed parents for state transition
        public final AtomicInteger[] fd_countdown;
        public final AtomicInteger[] td_countdown;
        public final AtomicInteger[] ld_countdown;
        public final AtomicInteger[] ld_spec_countdown;

        // countdown to be committable descendants
        public final AtomicInteger ld_descendant_countdown;

        /**
         * A flag to control the state transition from a BLOCKED operation
         * it can be either SPEC/READY
         * 0 cannot be ready or spec, this is for the safety consideration
         * 1 can promote to a spec
         * 2 can promote to a ready
         */
        public final AtomicInteger is_spec_or_ready_candidate;

        public OperationMetadata() {
            td_countdown = new AtomicInteger[2]; // countdown Idx0/1 when executed/committed
            fd_countdown = new AtomicInteger[1]; // countdown when executed
            ld_countdown = new AtomicInteger[1]; // countdown when executed
            ld_spec_countdown = new AtomicInteger[1]; // countdown when executed

            // initialize those atomic integer val/arr
            initializeAtomicIntegerArr(td_countdown);
            initializeAtomicIntegerArr(fd_countdown);
            initializeAtomicIntegerArr(ld_countdown);
            initializeAtomicIntegerArr(ld_spec_countdown);
            ld_descendant_countdown = new AtomicInteger(0);

            is_spec_or_ready_candidate = new AtomicInteger(NONE_CANDIDATE);
        }

        private void initializeAtomicIntegerArr(AtomicInteger[] arr) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new AtomicInteger(0);
            }
        }
    }
}