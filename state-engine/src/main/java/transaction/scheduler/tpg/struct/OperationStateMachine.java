package transaction.scheduler.tpg.struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: clean ``state" and ``reference".
 */
public abstract class OperationStateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(OperationStateMachine.class);

    private OperationStateMachine ld_head_operation;
    private final Queue<OperationStateMachine> ld_descendant_operations;

    private final Queue<OperationStateMachine> fd_children; // the finctional dependencies ops to be executed after this op.
    private final Queue<OperationStateMachine> td_children; // the finctional dependencies ops to be executed after this op.
    private final Queue<OperationStateMachine> ld_children; // the finctional dependencies ops to be executed after this op.
    private final Queue<OperationStateMachine> ld_spec_children; // speculative children to notify.

    private final Queue<OperationStateMachine> fd_parents; // the finctional dependnecies ops to be executed in advance
    private final Queue<OperationStateMachine> td_parents; // the finctional dependnecies ops to be executed in advance
    private final Queue<OperationStateMachine> ld_parents; // the finctional dependnecies ops to be executed in advance
    private final Queue<OperationStateMachine> ld_spec_parents; // speculative parents to wait, include the last ready op

    private final OperationMetadata operationMetadata;

    private final AtomicReference<MetaTypes.OperationStateType> operationState;

    public final static int NONE_CANDIDATE = 0;
    public final static int SPECULATIVE_CANDIDATE = 1;
    public final static int READY_CANDIDATE = 2;

    public OperationStateMachine() {
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

        operationMetadata = new OperationMetadata();

        operationState = new AtomicReference<>(MetaTypes.OperationStateType.BLOCKED);
    }

    public <T extends OperationStateMachine> ArrayDeque<T> getChildren(MetaTypes.DependencyType type) {
        if (type.equals(MetaTypes.DependencyType.FD)) {
            return (ArrayDeque<T>) fd_children;
        } else if (type.equals(MetaTypes.DependencyType.TD)) {
            return (ArrayDeque<T>) td_children;
        } else if (type.equals(MetaTypes.DependencyType.LD)) {
            return (ArrayDeque<T>) ld_children;
        } else if (type.equals(MetaTypes.DependencyType.SP_LD)) {
            return (ArrayDeque<T>) ld_spec_children;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }

    public boolean isRoot() {
        return operationMetadata.td_countdown[0].get() == 0 && operationMetadata.fd_countdown[0].get() == 0 && operationMetadata.ld_spec_countdown[0].get() == 0;
    }

    public void addParent(OperationStateMachine operation, MetaTypes.DependencyType type) {
        if (type.equals(MetaTypes.DependencyType.FD)) {
            this.fd_parents.add(operation);
            this.operationMetadata.fd_countdown[0].incrementAndGet();
        } else if (type.equals(MetaTypes.DependencyType.LD)) {
            this.ld_parents.add(operation);
            this.operationMetadata.ld_countdown[0].incrementAndGet();
        } else if (type.equals(MetaTypes.DependencyType.SP_LD)) {
            this.ld_spec_parents.add(operation);
            this.operationMetadata.ld_spec_countdown[0].incrementAndGet();
        } else if (type.equals(MetaTypes.DependencyType.TD)) {
            this.td_parents.add(operation);
            this.operationMetadata.td_countdown[0].incrementAndGet();
            this.operationMetadata.td_countdown[1].incrementAndGet();
        } else {
            throw new RuntimeException("unsupported dependency type parent");
        }
    }

    public void addChild(OperationStateMachine operation, MetaTypes.DependencyType type) {
        if (type.equals(MetaTypes.DependencyType.FD)) {
            this.fd_children.add(operation);
        } else if (type.equals(MetaTypes.DependencyType.LD)) {
            this.ld_children.clear();
            this.ld_children.add(operation);
        } else if (type.equals(MetaTypes.DependencyType.SP_LD)) {
            this.ld_spec_children.add(operation);
        } else if (type.equals(MetaTypes.DependencyType.TD)) {
            this.td_children.add(operation);
        } else {
            throw new RuntimeException("unsupported dependency type children");
        }
    }

    public void addHeader(OperationStateMachine header) {
        ld_head_operation = header;
    }

    public void addDescendant(OperationStateMachine descendant) {
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

    public void stateTransition(MetaTypes.OperationStateType state) {
            LOG.debug(this + " : state transit " + operationState + " -> " + state);
        operationState.getAndSet(state);
    }

    public MetaTypes.OperationStateType getOperationState() {
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
     * @param dependencyType
     * @param parentState
     */
    public void updateDependencies(MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        // update countdown
        if (parentState.equals(MetaTypes.OperationStateType.EXECUTED)) {
            if (dependencyType.equals(MetaTypes.DependencyType.TD)) {
                operationMetadata.td_countdown[0].decrementAndGet();
                assert operationMetadata.td_countdown[0].get() >= 0;
            } else if (dependencyType.equals(MetaTypes.DependencyType.FD)) {
                operationMetadata.fd_countdown[0].decrementAndGet();
                assert operationMetadata.fd_countdown[0].get() >= 0;
            } else if (dependencyType.equals(MetaTypes.DependencyType.SP_LD)) {
                operationMetadata.ld_spec_countdown[0].decrementAndGet();
            }
        } else if (parentState.equals(MetaTypes.OperationStateType.COMMITTED)) {
            if (dependencyType.equals(MetaTypes.DependencyType.TD)) {
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
                    && this.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED);
        else
            isSpeculative = isSpeculative &&
                    this.getOperationState().equals(MetaTypes.OperationStateType.BLOCKED);
        if (isSpeculative) {
            stateTransition(MetaTypes.OperationStateType.SPECULATIVE);
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
                    && this.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED);
        else
            isReady = isReady
                    && (this.getOperationState().equals(MetaTypes.OperationStateType.BLOCKED));
        if (isReady) {
            stateTransition(MetaTypes.OperationStateType.READY);
        }
        return isReady;
    }

    public boolean tryHeaderCommit() {
        // header check whether all descendants are in committable, if so, commit.
        if (!this.equals(ld_head_operation)) {
            throw new RuntimeException("this can be only invoked by header.");
        }
        LOG.debug("++++++" + this + " check whether committable: " + ld_descendant_operations);
        boolean readyToCommit = this.getOperationState().equals(MetaTypes.OperationStateType.COMMITTABLE);
        readyToCommit = readyToCommit && operationMetadata.ld_descendant_countdown.get()==0;
        if (readyToCommit) {
            // notify all descendants to commit.
            stateTransition(MetaTypes.OperationStateType.COMMITTED);
        }
        return readyToCommit;
    }

    public boolean tryCommittable() {
        LOG.debug("++++++" + this + " check committable:");
        boolean isCommittable =
                operationMetadata.td_countdown[1].get() == 0
                        && this.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED);
        LOG.debug("++++++" + this + " try commit results: " +
                        (operationMetadata.td_countdown[1].get() == 0)
                        + " | " + this.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED));
        if (isCommittable) {
            // EXECUTED->COMMITTABLE
            stateTransition(MetaTypes.OperationStateType.COMMITTABLE);
        } else {
            LOG.debug("++++++" + this + " committable failed");
        }
        LOG.debug("++++++" + this + " check committable end");
        return isCommittable;
    }

    protected abstract boolean checkOthertxnOperation(Queue<OperationStateMachine> td_parents, MetaTypes.OperationStateType targetState);


    // **********************************Utilities**********************************
    /**
     * @param type
     * @param <T>
     * @return
     */
    public <T extends OperationStateMachine> Queue<T> getParents(MetaTypes.DependencyType type) {
        if (type.equals(MetaTypes.DependencyType.FD)) {
            return (Queue<T>) fd_parents;
        } else if (type.equals(MetaTypes.DependencyType.TD)) {
            return (Queue<T>) td_parents;
        } else if (type.equals(MetaTypes.DependencyType.LD)) {
            return (Queue<T>) ld_parents;
        } else {
            throw new RuntimeException("Unexpected dependency type: " + type);
        }
    }

    public <T extends OperationStateMachine> T getHeader() {
        return (T) ld_head_operation;
    }

    public <T extends OperationStateMachine> ArrayDeque<T> getDescendants() {
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

        // promote header to be the first ready candidate of the transaction
        public void setReadyCandidate() {
            is_spec_or_ready_candidate.getAndSet(READY_CANDIDATE);
        }

        public void setSpeculativeCandidate() {
            is_spec_or_ready_candidate.compareAndSet(NONE_CANDIDATE, SPECULATIVE_CANDIDATE);
        }
    }
}