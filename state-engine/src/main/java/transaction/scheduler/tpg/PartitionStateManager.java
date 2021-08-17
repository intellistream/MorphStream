package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.og.*;
import transaction.scheduler.tpg.signal.og.OnRootSignal;
import transaction.scheduler.tpg.signal.op.*;
import transaction.scheduler.tpg.struct.*;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.MetaTypes.OperationStateType;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements OperationStateListener, Runnable, OperationGroupStateListener {
    public final ArrayList<String> partition; //  list of states being responsible for
    public final Queue<OperationSignal> opSignalQueue;
    public final Queue<OperationGroupSignal> ogSignalQueue;
    private TaskPrecedenceGraph.ShortCutListener shortCutListener;

    public PartitionStateManager() {
        this.opSignalQueue = new ConcurrentLinkedQueue<>();
        this.ogSignalQueue = new ConcurrentLinkedQueue<>();
        this.partition = new ArrayList<>();
    }

    @Override
    public void onOpParentStateUpdated(Operation operation, DependencyType dependencyType, OperationStateType parentState) {
        opSignalQueue.add(new OnParentCommittedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onReadyParentStateUpdated(Operation operation, DependencyType dependencyType, OperationStateType parentState) {
        opSignalQueue.add(new OnReadyParentExecutedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onOpHeaderStateUpdated(Operation operation, OperationStateType headerState) {
        opSignalQueue.add(new OnHeaderUpdatedSignal(operation, headerState));//
    }

    @Override
    public void onOpDescendantStateUpdated(Operation operation, OperationStateType descendantState) {
        opSignalQueue.add(new OnDescendantUpdatedSignal(operation, descendantState));//
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    public void handleStateTransitions() {
        OperationGroupSignal ogSignal = ogSignalQueue.poll();
        while (ogSignal != null) {
            OperationGroup operationGroup = ogSignal.getTargetOperationGroup();
            if (ogSignal instanceof OnRootSignal) {
                ogRootStartTransition(operationGroup);
            } else if (ogSignal instanceof OnExecutedSignal) {
                ogExecutedTransition(operationGroup);
            } else if (ogSignal instanceof OnParentExecutedSignal) {
                ogParentExecutedTransition(operationGroup,
                        ((OnParentExecutedSignal) ogSignal).getDependencyType());
            } else if (ogSignal instanceof OnRollbackAndRedoSignal) {
                ogRollbackAndRedoTransition(operationGroup,
                        ((OnRollbackAndRedoSignal) ogSignal).getAbortedOperation());
            } else if (ogSignal instanceof OnParentWithAbortedRollbackAndRedoSignal) {
                ogParentWithAbortedTransition(operationGroup,
                        ((OnParentWithAbortedRollbackAndRedoSignal) ogSignal).getTargetChildOperation(),
                        ((OnParentWithAbortedRollbackAndRedoSignal) ogSignal).getAbortedOperation(),
                        ((OnParentWithAbortedRollbackAndRedoSignal) ogSignal).getDependencyType());
            } else if (ogSignal instanceof OnParentRollbackAndRedoSignal) {
                ogParentRollbackAndRedoTransition(operationGroup);
            }
            ogSignal = ogSignalQueue.poll();
        }

        // do transaction commit and transaction abort
        OperationSignal opSignal = opSignalQueue.poll();
        while (opSignal != null) {
            Operation operation = opSignal.getTargetOperation();
            if (opSignal instanceof OnParentCommittedSignal) {
                onParentStateCommittedTransition(operation, (OnParentCommittedSignal) opSignal);
            } else if (opSignal instanceof OnDescendantUpdatedSignal) {
                onDescendantUpdatedTransition(operation, (OnDescendantUpdatedSignal) opSignal);
            } else if (opSignal instanceof OnHeaderUpdatedSignal) {
                onHeaderUpdatedTransition(operation, (OnHeaderUpdatedSignal) opSignal);
            } else {
                throw new UnsupportedOperationException("unknown signal received. " + opSignal);
            }
            opSignal = opSignalQueue.poll();
        }
    }

    private void onHeaderUpdatedTransition(Operation descendant, OnHeaderUpdatedSignal signal) {
        OperationStateType headerState = signal.getState();
        if (headerState.equals(OperationStateType.COMMITTED)) {
            descendant.stateTransition(OperationStateType.COMMITTED);
            committedAction(descendant);
        } else if (headerState.equals(OperationStateType.ABORTED)) {
            descendant.stateTransition(OperationStateType.ABORTED);
            abortedAction(descendant);
        }
    }

    private void committedAction(Operation operation) {
        shortCutListener.onOperationFinalized(operation, true);
        // notify all td children committed
        Queue<Operation> children = operation.getChildren(DependencyType.TD);
        for (Operation child : children) {
            child.context.partitionStateManager.onOpParentStateUpdated(child, DependencyType.TD, OperationStateType.COMMITTED);
        }
        // notify all FD children committed
        children = operation.getChildren(DependencyType.FD);
        for (Operation child : children) {
            child.context.partitionStateManager.onOpParentStateUpdated(child, DependencyType.FD, OperationStateType.COMMITTED);
        }
    }

    private void abortedAction(Operation operation) {
        shortCutListener.onOperationFinalized(operation, false);
        operation.getOG().context.partitionStateManager.onOgRollbackAndRedo(operation.getOG(), operation);
    }

    private void scheduleCommitOrAbort(Operation operation, OperationStateType finalState) {
        assert operation.isHeader();
        // notify descendants to commit.
        Queue<Operation> descendants = operation.getDescendants();
        for (Operation descendant : descendants) {
            descendant.context.partitionStateManager.onOpHeaderStateUpdated(descendant, finalState);
        }
    }

    private void onDescendantUpdatedTransition(Operation header, OnDescendantUpdatedSignal signal) {
        assert header.isHeader();
        OperationStateType descendantsState = signal.getState();
        if (descendantsState.equals(OperationStateType.COMMITTABLE)) {
            header.updateCommitCountdown();
            if (header.tryHeaderCommit()) {
                scheduleCommitOrAbort(header, OperationStateType.COMMITTED);
            }
        } else if (descendantsState.equals(OperationStateType.ABORTED)) {
            scheduleCommitOrAbort(header, OperationStateType.ABORTED);
        }
    }

    private void onParentStateCommittedTransition(Operation operation, OnParentCommittedSignal signal) {
        DependencyType dependencyType = signal.getType();
        OperationStateType parentState = signal.getState();
        operation.updateDependencies(dependencyType, parentState);
        if (operation.getOperationState().equals(OperationStateType.EXECUTED)) {
            if (operation.tryCommittable()) {
                executedToCommittableAction(operation);
            }
        }
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onOpDescendantStateUpdated(header, OperationStateType.COMMITTABLE);
    }

    /** OC related listener method and transitions
     * @param operationGroup**/

    @Override
    public void onOgRootStart(OperationGroup operationGroup) {
        ogSignalQueue.add(new OnRootSignal(operationGroup));
    }

    @Override
    public void onOgExecuted(OperationGroup operationGroup) {
        ogSignalQueue.add(new OnExecutedSignal(operationGroup));
    }

    @Override
    public void onOgParentExecuted(OperationGroup operationGroup, DependencyType dependencyType) {
        ogSignalQueue.add(new OnParentExecutedSignal(operationGroup, dependencyType));
    }

    @Override
    public void onOgRollbackAndRedo(OperationGroup operationGroup, Operation abortedOperation) {
        ogSignalQueue.add(new OnRollbackAndRedoSignal(operationGroup, abortedOperation));
    }

    public void onOgParentWithAborted(OperationGroup operationGroup, DependencyType dependencyType, Operation child, Operation abortedOperation) {
        ogSignalQueue.add(new OnParentWithAbortedRollbackAndRedoSignal(operationGroup, dependencyType, child, abortedOperation));
    }

    @Override
    public void onOgParentRollbackAndRedo(OperationGroup operationGroup, DependencyType dependencyType) {
        ogSignalQueue.add(new OnParentRollbackAndRedoSignal(operationGroup, dependencyType));
    }



    private void ogRootStartTransition(OperationGroup operationGroup) {
        shortCutListener.onExecutable(operationGroup);
    }

    private void ogExecutedTransition(OperationGroup operationGroup) {
        // update status of each operation
        operationGroup.isExecuted = true;
        boolean isFailed = false;
        for (Operation operation : operationGroup.getOperations()) {
            // TODO: operation may be rollbacked when it is in the executable queue
            if (!operation.isFailed) {
                operation.stateTransition(OperationStateType.EXECUTED);
                if (operation.tryCommittable()) {
                    executedToCommittableAction(operation);
                }
            } else {
                operation.stateTransition(OperationStateType.ABORTED);
                executionFailedAction(operation);
                isFailed = true;
            }
        }

        // if is failed, do not need to notify the subsequent operations
        if (isFailed) return;
        for (Operation child : operationGroup.getFdChildren()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.FD);
        }
        for (Operation child : operationGroup.getLdChildren()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.LD);
        }
        if (operationGroup.getTdChild() != null) {
            operationGroup.getTdChild().context.partitionStateManager.onOgParentExecuted(operationGroup.getTdChild(), DependencyType.TD);
        }
        shortCutListener.onOGFinalized(operationGroup.getOperationGroupId());
    }

    private void executionFailedAction(Operation operation) {
        // update the operations in the operation group, delete the committable operations and redo the operation.
        // 1. operation notify header to abort
        // 2. update operation group operations
        // 3. operation group notify children groups for rollback
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onOpDescendantStateUpdated(header, OperationStateType.ABORTED);
    }

    /**
     * The operation group is informed to rollback and red, by a "header operation of a transaction".
     * If it is already executed, it will delete the finalized operations and rollback and redo. Inform its children operation groups to rollback and redo.
     *
     * @param operationGroup
     * @param abortedOperation
     */
    private void ogRollbackAndRedoTransition(OperationGroup operationGroup, Operation abortedOperation) {
        rollbackOperations(operationGroup);
        // try to update the dependency of each child operation for aborted operation
        Queue<Operation> op_td_children = abortedOperation.getChildren(DependencyType.TD);
        for (Operation op_td_child : op_td_children) {
            if (op_td_child.getOG().equals(operationGroup)) { // in the same operation group
                op_td_child.updateDependencies(DependencyType.TD, OperationStateType.ABORTED);
            } else {
                op_td_child.context.partitionStateManager.onOgParentWithAborted(op_td_child.getOG(), DependencyType.TD, op_td_child, abortedOperation);
            }
        }
        Queue<Operation> op_fd_children = abortedOperation.getChildren(DependencyType.FD);
        for (Operation op_fd_child : op_fd_children) {
            op_fd_child.context.partitionStateManager.onOgParentWithAborted(op_fd_child.getOG(), DependencyType.FD, op_fd_child, abortedOperation);
        }

        // rollback and redo if operation is executed, and inform its children og to rollback and redo
        if (operationGroup.isExecuted) {
            operationGroup.isExecuted = false;
            if (!operationGroup.hasParents()) {
                shortCutListener.onExecutable(operationGroup);
            }
            // notify its children the aborted operations
            for (Operation child : operationGroup.getFdChildren()) {
                child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.FD);
            }
            if (operationGroup.getTdChild() != null) {
                operationGroup.getTdChild().context.partitionStateManager.onOgParentRollbackAndRedo(operationGroup.getTdChild(), DependencyType.TD);
            }
//            for (Operation child : operationGroup.getLdChildren()) { // TODO: do we really need to let LD in other txn to rollback and redo?
//                if (child != abortedOperation) {
//                    child.context.partitionStateManager.onOgParentRollbackAndRedo(operationGroup, DependencyType.LD);
//                }
//            }
        }
    }

    /**
     * The operation group is informed by its "parent operation group with aborted operations" of rollback and redo.
     * The operation group will update the countdown of operations that are the children of the aborted operations.
     * The operation group will reset its dependencies
     * If it is already executed, it will delete the finalized operations and rollback and redo. Inform its children operation groups to rollback and redo.
     * @param operationGroup
     * @param targetChildOperation
     * @param abortedOperation
     * @param dependencyType
     */
    private void ogParentWithAbortedTransition(OperationGroup operationGroup, Operation targetChildOperation, Operation abortedOperation, DependencyType dependencyType) {
        targetChildOperation.updateDependencies(dependencyType, OperationStateType.ABORTED);
//        if (operationGroup.isExecuted) {
//            removeUnrollbackableOperation(operationGroup);
//            operationGroup.resetDependencies();
//            operationGroup.isExecuted = false;
//            if (!operationGroup.hasParents()) {
//                shortCutListener.onExecutable(operationGroup);
//            }
//            // notify its children rollback and redo
//            for (Operation child : operationGroup.getFdChildren()) {
//                child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.FD);
//            }
//            for (Operation child : operationGroup.getLdChildren()) {
//                child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.LD);
//            }
//        }
    }

    /**
     * The operation group is informed by its "parent operation group without aborted operations" rollback and redo.
     * The operation group will reset its dependencies
     * if it is already executed, it will delete the finalized operations and rollback and redo. Inform its children operation groups to rollback and redo.
     *
     * @param operationGroup
     */
    private void ogParentRollbackAndRedoTransition(OperationGroup operationGroup) {
        rollbackOperations(operationGroup);
        operationGroup.resetDependencies();
        if (operationGroup.isExecuted) {
            operationGroup.isExecuted = false;
            // notify its children rollback and redo
            for (Operation child : operationGroup.getFdChildren()) {
                child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.FD);
            }
            if (operationGroup.getTdChild() != null) {
                operationGroup.getTdChild().context.partitionStateManager.onOgParentRollbackAndRedo(operationGroup.getTdChild(), DependencyType.TD);
            }
        }
    }

    private void rollbackOperations(OperationGroup operationGroup) {
        List<Operation> removedOperations = new ArrayList<>();
        for (Operation operation : operationGroup.getOperations()) { //TODO: traverse can be optimized
            if (operation.unRedoable()) {
                removedOperations.add(operation);
            } else {
                operation.stateTransition(OperationStateType.BLOCKED); // otherwise it should always rollback to blocked
            }
        }
        operationGroup.removeOperations(removedOperations);
    }

//    private void removeUnrollbackableOperation(OperationGroup operationGroup) {
//        List<Operation> removedOperations = new ArrayList<>();
//        for (Operation operation : operationGroup.getOperations()) { //TODO: traverse can be optimized
//            if (operation.unRedoable()) {
//                removedOperations.add(operation);
//            } else {
//                operation.stateTransition(OperationStateType.BLOCKED); // otherwise it should always rollback to blocked
//            }
//        }
//        operationGroup.removeOperations(removedOperations);
//    }

    private void ogParentExecutedTransition(OperationGroup operationGroup, DependencyType dependencyType) {
        operationGroup.updateDependencies(dependencyType);
        if (!operationGroup.hasParents()) {
            shortCutListener.onExecutable(operationGroup);
        }
    }

    public void initialize(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
        // 1. set listener
        this.shortCutListener = shortCutListener;
    }
}
