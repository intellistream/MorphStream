package engine.txn.scheduler.struct.op;

import engine.txn.content.common.CommonMetaTypes;
import engine.txn.scheduler.context.op.OPSContext;
import engine.txn.scheduler.context.op.OPSchedulerContext;
import engine.txn.transaction.context.TxnContext;
import engine.txn.transaction.function.Condition;
import engine.txn.transaction.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.scheduler.signal.op.OnParentUpdatedSignal;
import engine.txn.scheduler.struct.AbstractOperation;
import engine.txn.scheduler.struct.op.MetaTypes.DependencyType;
import engine.txn.scheduler.struct.op.MetaTypes.OperationStateType;
import engine.txn.storage.SchemaRecordRef;
import engine.txn.storage.TableRecord;
import engine.txn.storage.table.BaseTable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class Operation extends AbstractOperation implements Comparable<Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
    public final OPSchedulerContext context;

    private final Deque<Operation> ld_descendant_operations;
    public final Deque<Operation> fd_children; // NOTE: this is concurrently constructed, so need to use concurrent structure
    public final Deque<Operation> td_children; // the functional dependencies ops to be executed after this op.
    private final Deque<Operation> ld_children; // the functional dependencies ops to be executed after this op.
    private final Deque<Operation> ld_spec_children; // speculative children to notify.
    public final Deque<Operation> fd_parents; // the functional dependencies ops to be executed in advance.
    public final Deque<Operation> td_parents; // the functional dependencies ops to be executed in advance
    private final Deque<Operation> ld_parents; // the functional dependencies ops to be executed in advance
    private final OperationMetadata operationMetadata;
    private OperationStateType operationState;
    // operation id under a transaction.
    // an operation id to indicate how many operations in front of this operation in the same transaction.
    public int txnOpId = 0;
    public boolean isFailed;
    public String name;

    // operation group the operation belongs to, it can be used for communication of operation groups.
    private AbstractOperation ld_head_operation;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public String[] condition_sourceTable = null;
    public String[] condition_source = null;
    public boolean isNonDeterministicOperation = false;
    public BaseTable[] tables;
    public TableRecord[] deterministicRecords;

    /****************************Defined by MYC*************************************/

    public <Context extends OPSchedulerContext> Operation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                          CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success, null);
    }

    public <Context extends OPSchedulerContext> Operation(
            String pKey, Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        this(pKey, context, table_name, txn_context, bid, accessType, record, record_ref, function, condition, condition_records, success, null);
    }

    public <Context extends OPSchedulerContext> Operation(
            String pKey, Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success, WindowDescriptor windowContext) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid, windowContext, pKey);
        this.context = context;

        ld_head_operation = null;
        ld_descendant_operations = new ArrayDeque<>();

        // finctional dependencies, this should be concurrent because cross thread access.
        fd_parents = new ConcurrentLinkedDeque<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ConcurrentLinkedDeque<>(); // the finctional dependencies ops to be executed after this op.
        // temporal dependencies
        td_parents = new ConcurrentLinkedDeque<>(); // the finctional dependnecies ops to be executed in advance
        td_children = new ConcurrentLinkedDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        ld_parents = new ArrayDeque<>(); // the finctional dependnecies ops to be executed in advance
        ld_children = new ArrayDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        // speculative parents to wait, include the last ready op
        // speculative parents to wait, include the last ready op
        ld_spec_children = new ArrayDeque<>(); // speculative children to notify.

        operationMetadata = new OperationMetadata();
//        operationState = new AtomicReference<>(OperationStateType.BLOCKED);
        operationState = OperationStateType.BLOCKED;
    }
    public <Context extends OPSchedulerContext> Operation(Boolean isNonDeterministicOperation, BaseTable[] tables,
                                                          String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                          CommonMetaTypes.AccessType accessType, TableRecord record,
                                                          SchemaRecordRef record_ref, Function function, Condition condition,
                                                          TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid, null, pKey);
        this.context = context;
        this.isNonDeterministicOperation = isNonDeterministicOperation;
        this.tables = tables;
        ld_head_operation = null;
        ld_descendant_operations = new ArrayDeque<>();

        // finctional dependencies, this should be concurrent because cross thread access
        fd_parents = new ConcurrentLinkedDeque<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ConcurrentLinkedDeque<>(); // the finctional dependencies ops to be executed after this op.
        // temporal dependencies
        td_parents = new ConcurrentLinkedDeque<>(); // the finctional dependnecies ops to be executed in advance
        td_children = new ConcurrentLinkedDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        ld_parents = new ArrayDeque<>(); // the finctional dependnecies ops to be executed in advance
        ld_children = new ArrayDeque<>(); // the finctional dependencies ops to be executed after this op.
        // finctional dependencies
        // speculative parents to wait, include the last ready op
        // speculative parents to wait, include the last ready op
        ld_spec_children = new ArrayDeque<>(); // speculative children to notify.

        operationMetadata = new OperationMetadata();
//        operationState = new AtomicReference<>(OperationStateType.BLOCKED);
        operationState = OperationStateType.BLOCKED;
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
                return this.getTxnOpId() - operation.getTxnOpId();
            }
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    @Override
    public String toString() {
        return bid + "|" + txnOpId + "|" + String.format("%-15s", this.getOperationState());
    }

    public void setTxnOpId(int op_id) {
        this.txnOpId = op_id;
    }

    public int getTxnOpId() {
        return txnOpId;
    }

    public boolean isRoot() {
        return operationMetadata.td_countdown.get() == 0 && operationMetadata.fd_countdown.get() == 0 && operationMetadata.ld_countdown.get() == 0;
    }

    public void setConditionSources(String[] condition_sourceTable, String[] condition_source) {
        this.condition_sourceTable = condition_sourceTable;
        this.condition_source = condition_source;
    }

    public void initialize() {
        operationMetadata.fd_countdown.set(fd_parents.size());
        operationMetadata.td_countdown.set(td_parents.size());
        operationMetadata.ld_countdown.set(ld_parents.size());
    }

    public void addParent(Operation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_parents.add(operation);
//            this.operationMetadata.fd_countdown++;
        } else if (type.equals(DependencyType.LD)) {
            this.ld_parents.add(operation);
//            this.operationMetadata.ld_countdown++;
        } else if (type.equals(DependencyType.TD)) {
            this.td_parents.add(operation);
//            this.operationMetadata.td_countdown++;
        } else {
            throw new RuntimeException("unsupported dependency type parent");
        }
    }

    public void addChild(Operation operation, DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            this.fd_children.add(operation);
        } else if (type.equals(DependencyType.LD)) {
            this.ld_children.add(operation);
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
    }

    public void stateTransition(OperationStateType state) {
//        LOG.debug(this + " : state transit " + operationState + " -> " + state);
//        operationState.getAndSet(state);
        operationState = state;
    }

    public OperationStateType getOperationState() {
//        return operationState;
        return operationState;
    }


//    // promote header to be the first ready candidate of the transaction
//    public void setReadyCandidate() {
//        operationMetadata.is_spec_or_ready_candidate.getAndSet(READY_CANDIDATE);
//    }
//
//    public void setSpeculativeCandidate() {
//        operationMetadata.is_spec_or_ready_candidate.compareAndSet(NONE_CANDIDATE, SPECULATIVE_CANDIDATE);
//    }

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
                operationMetadata.td_countdown.decrementAndGet();
                assert operationMetadata.td_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.FD)) {
                operationMetadata.fd_countdown.decrementAndGet();
                assert operationMetadata.fd_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.LD)) {
                operationMetadata.ld_countdown.decrementAndGet();
                assert operationMetadata.ld_countdown.get() >= 0;
            }
        } else if (parentState.equals(OperationStateType.READY) || parentState.equals(OperationStateType.BLOCKED)) { // rollback
            if (dependencyType.equals(DependencyType.TD)) {
                operationMetadata.td_countdown.incrementAndGet();
                assert operationMetadata.td_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.FD)) {
                operationMetadata.fd_countdown.incrementAndGet();
                assert operationMetadata.fd_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.LD)) {
                operationMetadata.ld_countdown.incrementAndGet();
                assert operationMetadata.ld_countdown.get() >= 0;
            }
        } else if (parentState.equals(OperationStateType.ABORTED)) {
            if (dependencyType.equals(DependencyType.TD)) {
//                operationMetadata.td_countdown.set(td_parents.size());
//                for (Operation operation : td_parents) {
//                    if (operation.getOperationState().equals(OperationStateType.EXECUTED)
//                        || operation.getOperationState().equals(OperationStateType.ABORTED)) {
//                        operationMetadata.td_countdown--;
//                    }
//                }
                operationMetadata.td_countdown.decrementAndGet();
                assert operationMetadata.td_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.FD)) {
//                operationMetadata.fd_countdown.set(fd_parents.size());
//                for (Operation operation : fd_parents) {
//                    if (operation.getOperationState().equals(OperationStateType.EXECUTED)
//                            || operation.getOperationState().equals(OperationStateType.ABORTED)) {
//                        operationMetadata.fd_countdown--;
//                    }
//                }
                operationMetadata.fd_countdown.decrementAndGet();
                assert operationMetadata.fd_countdown.get() >= 0;
            } else if (dependencyType.equals(DependencyType.LD)) {
//                operationMetadata.ld_countdown.set(ld_parents.size());
//                for (Operation operation : ld_parents) {
//                    if (operation.getOperationState().equals(OperationStateType.EXECUTED)
//                            || operation.getOperationState().equals(OperationStateType.ABORTED)) {
//                        operationMetadata.ld_countdown--;
//                    }
//                }
                operationMetadata.ld_countdown.decrementAndGet();
                assert operationMetadata.ld_countdown.get() >= 0;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void resetDependencies() {
        operationMetadata.td_countdown.set(td_parents.size());
        operationMetadata.fd_countdown.set(fd_parents.size());
        operationMetadata.ld_countdown.set(ld_parents.size());
    }


    public boolean tryReady() {
        boolean isReady = operationMetadata.td_countdown.get() == 0
                && operationMetadata.fd_countdown.get() == 0
                && operationMetadata.ld_countdown.get() == 0;

        if (isReady) {
            stateTransition(OperationStateType.READY);
        }
        return isReady;
    }

    // **********************************Utilities**********************************

    public boolean hasParents() {
        return !(operationMetadata.td_countdown.get() == 0 && operationMetadata.fd_countdown.get() == 0 && operationMetadata.ld_countdown.get() == 0);
    }

    /**
     * @param type
     * @param <T>
     * @return
     */
    public <T extends Operation> Queue<T> getParents(DependencyType type) {
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

    public <T extends Operation> Queue<T> getChildren(DependencyType type) {
        if (type.equals(DependencyType.FD)) {
            return (Queue<T>) fd_children;
        } else if (type.equals(DependencyType.TD)) {
            return (Queue<T>) td_children;
        } else if (type.equals(DependencyType.LD)) {
            return (Queue<T>) ld_children;
        } else if (type.equals(DependencyType.SP_LD)) {
            return (Queue<T>) ld_spec_children;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }


    public <T extends Operation> T getHeader() {
        return (T) ld_head_operation;
    }

    public <T extends Operation> ArrayDeque<T> getDescendants() {
        return (ArrayDeque<T>) ld_descendant_operations;
    }

    public boolean isHeader() {
        return this.equals(ld_head_operation);
    }

    /********************************* Layered operation related interfaces *********************************/

//    public synchronized void updateDependencyLevel() {
//        if (isDependencyLevelCalculated)
//            return;
//        dependencyLevel = 0;
//        for (Operation parent : getParents(DependencyType.TD)) {
//            if (!parent.hasValidDependencyLevel()) {
//                parent.updateDependencyLevel();
//            }
//            if (parent.getDependencyLevel() >= dependencyLevel) {
//                dependencyLevel = parent.getDependencyLevel() + 1;
//            }
//        }
//        for (Operation parent : getParents(DependencyType.FD)) {
//            if (!parent.hasValidDependencyLevel()) {
//                parent.updateDependencyLevel();
//            }
//            if (parent.getDependencyLevel() >= dependencyLevel) {
//                dependencyLevel = parent.getDependencyLevel() + 1;
//            }
//        }
//        for (Operation parent : getParents(DependencyType.LD)) {
//            if (!parent.hasValidDependencyLevel()) {
//                parent.updateDependencyLevel();
//            }
//            if (parent.getDependencyLevel() >= dependencyLevel) {
//                dependencyLevel = parent.getDependencyLevel() + 1;
//            }
//        }
//        isDependencyLevelCalculated = true;
//    }

    public void layeredNotifyChildren() {
        for (Operation child : getChildren(DependencyType.TD)) {
            ((OPSContext) child.context).layerBuildHelperQueue
                    .add(new OnParentUpdatedSignal(child, DependencyType.TD, OperationStateType.EXECUTED));
        }
        for (Operation child : getChildren(DependencyType.FD)) {
            ((OPSContext) child.context).layerBuildHelperQueue
                    .add(new OnParentUpdatedSignal(child, DependencyType.FD, OperationStateType.EXECUTED));
        }
        for (Operation child : getChildren(DependencyType.LD)) {
            ((OPSContext) child.context).layerBuildHelperQueue
                    .add(new OnParentUpdatedSignal(child, DependencyType.LD, OperationStateType.EXECUTED));
        }
    }

    public void calculateDependencyLevelDuringExploration() {
        dependencyLevel = 0;
        for (Operation parent : getParents(DependencyType.TD)) {
            assert parent.hasValidDependencyLevel();
            if (parent.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = parent.getDependencyLevel() + 1;
            }
        }
        for (Operation parent : getParents(DependencyType.FD)) {
            assert parent.hasValidDependencyLevel();
            if (parent.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = parent.getDependencyLevel() + 1;
            }
        }
        for (Operation parent : getParents(DependencyType.LD)) {
            assert parent.hasValidDependencyLevel();
            if (parent.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = parent.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
        operationMetadata.td_countdown.set(td_parents.size());
        operationMetadata.fd_countdown.set(fd_parents.size());
        operationMetadata.ld_countdown.set(ld_parents.size());
    }

    public void updateDependencyLevel(int dependencyLevel) {
        this.dependencyLevel = dependencyLevel;
        isDependencyLevelCalculated = true;
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    public synchronized boolean hasValidDependencyLevel() {
        return isDependencyLevelCalculated;
    }

    public boolean hasChildren() {
        return !fd_children.isEmpty() || !td_children.isEmpty() || !ld_children.isEmpty();
    }


    private static class OperationMetadata {
        // countdown to be executed parents and committed parents for state transition
        // Has to use atomic for DFS, for notification from other threads, otherwise we may employ a state manager for DFS.
        public final AtomicInteger fd_countdown;
        public final AtomicInteger td_countdown;
        public final AtomicInteger ld_countdown;

//        public int fd_countdown;
//        public int td_countdown;
//        public int ld_countdown;

        /**
         * A flag to control the state transition from a BLOCKED operation
         * it can be either SPEC/READY
         * 0 cannot be ready or spec, this is for the safety consideration
         * 1 can promote to a spec
         * 2 can promote to a ready
         */
        public OperationMetadata() {
            td_countdown = new AtomicInteger(0); // countdown Idx0/1 when executed/committed
            fd_countdown = new AtomicInteger(0); // countdown when executed
            ld_countdown = new AtomicInteger(0); // countdown when executed
//            td_countdown = 0; // countdown Idx0/1 when executed/committed
//            fd_countdown = 0; // countdown when executed
//            ld_countdown = 0; // countdown when executed
        }
    }

}