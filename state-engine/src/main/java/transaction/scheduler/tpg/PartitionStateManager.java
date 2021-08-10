package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.*;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements OperationStateListener, Runnable {
    public final ArrayList<String> partition; //  list of states being responsible for
    //    public final HashMap<String, Queue<NotificationSignal>> stateTransitionQueueMap;
    public final Queue<NotificationSignal> stateTransitionQueue;
    private TaskPrecedenceGraph.ShortCutListener shortCutListener;

    public PartitionStateManager() {
        this.stateTransitionQueue = new ConcurrentLinkedQueue<>();
        this.partition = new ArrayList<>();
    }

    @Override
    public void onParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        stateTransitionQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onReadyParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        stateTransitionQueue.add(new OnReadyParentExecutedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onHeaderStateUpdated(Operation operation, MetaTypes.OperationStateType headerState) {
        stateTransitionQueue.add(new OnHeaderUpdatedSignal(operation, headerState));//
    }

    @Override
    public void onDescendantStateUpdated(Operation operation, MetaTypes.OperationStateType descendantState) {
        stateTransitionQueue.add(new OnDescendantUpdatedSignal(operation, descendantState));//
    }

    @Override
    public void onProcessed(Operation operation) {
//        PartitionStateManager partitionStateManager = getTargetStateManager(operation);
//        if (partitionStateManager.equals(this)) {
        stateTransitionQueue.add(new OnProcessedSignal(operation, operation.isFailed));//
//        } else {
//            partitionStateManager.onProcessed(operation);
//        }
    }

    public void onRootStart(Operation head) {
        stateTransitionQueue.add(new OnRootSignal(head));//
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    public void handleStateTransitions() {
//        inTransition.compareAndSet(false, true);
        NotificationSignal signal = stateTransitionQueue.poll();
        while (signal != null) {
            Operation operation = signal.getTargetOperation();
            if (signal instanceof OnProcessedSignal) {
                onProcessedTransition(operation, (OnProcessedSignal) signal);
            } else if (signal instanceof OnParentUpdatedSignal) {
                onParentStateUpdatedTransition(operation, (OnParentUpdatedSignal) signal);
            } else if (signal instanceof OnDescendantUpdatedSignal) {
                onDescendantUpdatedTransition(operation, (OnDescendantUpdatedSignal) signal);
            } else if (signal instanceof OnHeaderUpdatedSignal) {
                onHeaderUpdatedTransition(operation, (OnHeaderUpdatedSignal) signal);
            } else {
                throw new UnsupportedOperationException("unknow signal received. " + signal);
            }
            signal = stateTransitionQueue.poll();
        }
    }

    private void onHeaderUpdatedTransition(Operation descendant, OnHeaderUpdatedSignal signal) {
        MetaTypes.OperationStateType headerState = signal.getState();
        if (!descendant.isHeader()) {
            if (headerState.equals(MetaTypes.OperationStateType.COMMITTED)) {
                descendant.stateTransition(MetaTypes.OperationStateType.COMMITTED);
                committedAction(descendant);
            }
        }
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
        }
    }

    private void onParentStateUpdatedTransition(Operation operation, OnParentUpdatedSignal signal) {
        MetaTypes.DependencyType dependencyType = signal.getType();
        MetaTypes.OperationStateType parentState = signal.getState();
        operation.updateDependencies(dependencyType, parentState);
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)) {
            inExecutedState(operation, dependencyType, parentState);
        }
    }

    private void inExecutedState(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        if (dependencyType.equals(MetaTypes.DependencyType.TD)) {
            if (parentState.equals(MetaTypes.OperationStateType.COMMITTED)) {
                // when non ld parent is committed, the child should check whether to commit.
                if (operation.tryCommittable()) {
                    executedToCommittableAction(operation);
                }
            }
        }
    }


    private void onProcessedTransition(Operation operation, OnProcessedSignal signal) {
        if (signal.isFailed()) {
            // transit to aborted
//            operation.stateTransition(OperationStateType.ABORTED);
            System.out.println("++++++There will never be aborted operation" + operation);
            throw new UnsupportedOperationException("There will never be aborted operation");
            // TODO: notify children.
        } else {
            // transit to executed
            operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
            executedAction(operation);
            if (operation.tryCommittable()) {
                executedToCommittableAction(operation);
            }
        }
    }

    private void committedAction(Operation operation) {
        shortCutListener.onOperationFinalized(operation, true);
        if (operation.isHeader()) {
            // notify descendants to commit.
            ArrayDeque<Operation> descendants = operation.getDescendants();
            for (Operation descendant : descendants) {
                descendant.context.partitionStateManager.onHeaderStateUpdated(descendant, MetaTypes.OperationStateType.COMMITTED);
            }
        }
        // notify all td children committed
        ArrayDeque<Operation> children = operation.getChildren(MetaTypes.DependencyType.TD);
        for (Operation child : children) {
            child.context.partitionStateManager.onParentStateUpdated(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.COMMITTED);
        }
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onDescendantStateUpdated(header, MetaTypes.OperationStateType.COMMITTABLE);
    }

    private void blockedToSpeculativeAction(Operation operation) {
//        shortCutListener.onExecutable(operation, false);
        shortCutListener.onExecutable(operation, false);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        ArrayDeque<Operation> children = operation.getChildren(MetaTypes.DependencyType.SP_LD);
        if (operation.isReadyCandidate()) {
            // if is ready candidate and is executed, elect a new ready candidate
            if (!children.isEmpty()) {
                Operation child = children.getLast();
                child.context.partitionStateManager.onReadyParentStateUpdated(child, MetaTypes.DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
            }
        }
        // notify its ld speculative children to countdown, this countdown will only be used for ready candidate.
        // i.e. the read candidate will transit to ready when all its speculative countdown is 0;
        for (Operation child : children) {
            child.context.partitionStateManager.onParentStateUpdated(child, MetaTypes.DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
        }
    }

    public void initialize(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
        // 1. set listener
        this.shortCutListener = shortCutListener;
    }
}
