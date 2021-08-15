package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.og.*;
import transaction.scheduler.tpg.signal.og.OnRootSignal;
import transaction.scheduler.tpg.signal.op.*;
import transaction.scheduler.tpg.struct.*;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;

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
    public void onOpParentStateUpdated(Operation operation, DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opSignalQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onReadyParentStateUpdated(Operation operation, DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opSignalQueue.add(new OnReadyParentExecutedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onOpHeaderStateUpdated(Operation operation, MetaTypes.OperationStateType headerState) {
        opSignalQueue.add(new OnHeaderUpdatedSignal(operation, headerState));//
    }

    @Override
    public void onOpDescendantStateUpdated(Operation operation, MetaTypes.OperationStateType descendantState) {
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
                ogRollbackAndRedoTransition(operationGroup, true);
            } else if (ogSignal instanceof OnParentRollbackAndRedoSignal) {
                ogRollbackAndRedoTransition(operationGroup, false);
            }
            ogSignal = ogSignalQueue.poll();
        }

        // do transaction commit and transaction abort
        OperationSignal opSignal = opSignalQueue.poll();
        while (opSignal != null) {
            Operation operation = opSignal.getTargetOperation();
            if (opSignal instanceof OnParentUpdatedSignal) {
                onParentStateCommittedTransition(operation, (OnParentUpdatedSignal) opSignal);
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
        MetaTypes.OperationStateType headerState = signal.getState();
        if (!descendant.isHeader()) {
            if (headerState.equals(MetaTypes.OperationStateType.COMMITTED)) {
                descendant.stateTransition(MetaTypes.OperationStateType.COMMITTED);
                committedAction(descendant);
            } else if (headerState.equals(MetaTypes.OperationStateType.ABORTED)) {
                descendant.stateTransition(MetaTypes.OperationStateType.ABORTED);
                abortedAction(descendant);
            }
        }
    }

    private void abortedAction(Operation operation) {
        shortCutListener.onOperationFinalized(operation, false);
        if (operation.isHeader()) {
            // notify descendants to commit.
            Queue<Operation> descendants = operation.getDescendants();
            for (Operation descendant : descendants) {
                descendant.context.partitionStateManager.onOpHeaderStateUpdated(descendant, MetaTypes.OperationStateType.COMMITTED);
            }
        }
        operation.getOG().context.partitionStateManager.onOgRollbackAndRedo(operation.getOG());
    }

    private void onDescendantUpdatedTransition(Operation header, OnDescendantUpdatedSignal signal) {
        MetaTypes.OperationStateType descendantsState = signal.getState();
        header.updateCommitCountdown();

        if (descendantsState.equals(MetaTypes.OperationStateType.COMMITTABLE)) {
            if (header.tryHeaderCommit()) {
                committedAction(header);
            }
        } else if (descendantsState.equals(MetaTypes.OperationStateType.ABORTED)) {
            // TODO
            abortedAction(header);
        }
    }

    private void onParentStateCommittedTransition(Operation operation, OnParentUpdatedSignal signal) {
        DependencyType dependencyType = signal.getType();
        MetaTypes.OperationStateType parentState = signal.getState();
        operation.updateDependencies(dependencyType, parentState);
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)) {
            inExecutedState(operation, dependencyType, parentState);
        }
    }

    private void inExecutedState(Operation operation, DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        if (dependencyType.equals(DependencyType.TD)) {
            if (parentState.equals(MetaTypes.OperationStateType.COMMITTED)) {
                // when non ld parent is committed, the child should check whether to commit.
                if (operation.tryCommittable()) {
                    executedToCommittableAction(operation);
                }
            }
        }
    }


    private void committedAction(Operation operation) {
        shortCutListener.onOperationFinalized(operation, true);
        if (operation.isHeader()) {
            // notify descendants to commit.
            Queue<Operation> descendants = operation.getDescendants();
            for (Operation descendant : descendants) {
                descendant.context.partitionStateManager.onOpHeaderStateUpdated(descendant, MetaTypes.OperationStateType.COMMITTED);
            }
        }
        // notify all td children committed
        Queue<Operation> children = operation.getChildren(DependencyType.TD);
        for (Operation child : children) {
            child.context.partitionStateManager.onOpParentStateUpdated(child, DependencyType.TD, MetaTypes.OperationStateType.COMMITTED);
        }
        // notify all FD children committed
        children = operation.getChildren(DependencyType.FD);
        for (Operation child : children) {
            child.context.partitionStateManager.onOpParentStateUpdated(child, DependencyType.FD, MetaTypes.OperationStateType.COMMITTED);
        }
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onOpDescendantStateUpdated(header, MetaTypes.OperationStateType.COMMITTABLE);
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

    // TODO: rollback and redo
    public void onOgRollbackAndRedo(OperationGroup operationGroup) {
        ogSignalQueue.add(new OnRollbackAndRedoSignal(operationGroup));
    }

    public void onOgParentRollbackAndRedo(OperationGroup operationGroup, DependencyType dependencyType) {
        ogSignalQueue.add(new OnParentRollbackAndRedoSignal(operationGroup, dependencyType));
    }

    private void ogRootStartTransition(OperationGroup operationGroup) {
        shortCutListener.onExecutable(operationGroup);
    }

    private void ogExecutedTransition(OperationGroup operationGroup) {
        // update status of each operation
        operationGroup.isExecuted = true;
        for (Operation operation : operationGroup.getOperations()) {
            if (!operation.isFailed) {
                operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
                if (operation.tryCommittable()) {
                    executedToCommittableAction(operation);
                }
            } else {
                operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
                // update the operations in the operation group, delete the committable operations and redo the operation.
                // 1. operation notify header to abort
                // 2. update operation group operations
                // 3. operaion group notify children groups for rollback
                Operation header = operation.getHeader();
                header.context.partitionStateManager.onOpDescendantStateUpdated(operation, MetaTypes.OperationStateType.ABORTED);
                operationGroup.isExecuted = false;
            }
        }

        if (!operationGroup.isExecuted) {
            return;
        }

        for (Operation child : operationGroup.getFdChildren()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.FD);
        }
        for (Operation child : operationGroup.getLdChildren()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.LD);
        }

        shortCutListener.onOGFinalized(operationGroup.getOperationGroupId());
    }

    private void ogRollbackAndRedoTransition(OperationGroup operationGroup, boolean containsAbort) {
        List<Operation> removedOperations = new ArrayList<>();
        for (Operation operation : operationGroup.getOperations()) { //TODO: traverse can be optimized
            if (operation.getOperationState().equals(MetaTypes.OperationStateType.COMMITTED)
                || operation.getOperationState().equals(MetaTypes.OperationStateType.COMMITTABLE)
                || operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
                removedOperations.add(operation);
            }
        }
        operationGroup.removeOperations(removedOperations);
        if (containsAbort) {
            operationGroup.setupDependencies(containsAbort);
            shortCutListener.onExecutable(operationGroup);
        } else {
            operationGroup.setupDependencies(containsAbort);
        }

        // notify its children rollback and redo
        for (Operation child : operationGroup.getFdChildren()) {
            child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.FD);
        }
        for (Operation child : operationGroup.getLdChildren()) {
            child.context.partitionStateManager.onOgParentRollbackAndRedo(child.getOG(), DependencyType.LD);
        }
    }

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
