package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.*;
import transaction.scheduler.tpg.struct.Controller;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PartitionStateManager implements OperationStateListener, Runnable {
    private final int thread_id;
    public final Queue<NotificationSignal> stateTransitionQueue;
    private TaskPrecedenceGraph.ShortCutListener shortCutListener;


    public PartitionStateManager(int thread_id) {
        this.thread_id = thread_id;
        this.stateTransitionQueue = new ConcurrentLinkedQueue<>();
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
    public void onProcessed(Operation operation, boolean isFailed) {
        PartitionStateManager partitionStateManager = getTargetStateManager(operation);
        if (partitionStateManager.equals(this)) {
            stateTransitionQueue.add(new OnProcessedSignal(operation, isFailed));//
        } else {
            partitionStateManager.onProcessed(operation, isFailed);
        }
    }

    public void onRootStart(Operation operation) {
//        while (inTransition.get());
        PartitionStateManager partitionStateManager = getTargetStateManager(operation);
        if (partitionStateManager.equals(this)) {
            stateTransitionQueue.add(new OnRootSignal(operation));//
        } else {
            partitionStateManager.onRootStart(operation);
        }
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
//            System.out.println(thread_id + " : " + operation);
            if (signal instanceof OnProcessedSignal) {
                onProcessedTransition(operation, (OnProcessedSignal) signal);
            } else if (signal instanceof OnParentUpdatedSignal) {
                onParentStateUpdatedTransition(operation, (OnParentUpdatedSignal) signal);
            } else if (signal instanceof OnDescendantUpdatedSignal) {
                onDescendantUpdatedTransition(operation, (OnDescendantUpdatedSignal) signal);
            } else if (signal instanceof OnHeaderUpdatedSignal) {
                onHeaderUpdatedTransition(operation, (OnHeaderUpdatedSignal) signal);
            } else if (signal instanceof OnRootSignal) {
                onRootTransition(operation);
            } else if (signal instanceof OnReadyParentExecutedSignal) {
                onReadyParentUpdatedTransition(operation);
            }
            signal = stateTransitionQueue.poll();
        }
    }

    private void onReadyParentUpdatedTransition(Operation operation) {
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.BLOCKED)) {
            operation.setReadyCandidate();
            if (operation.tryReady(false)) {
                blockedToReadyAction(operation);
            }
        } else {
            throw new RuntimeException("operation cannot transit to ready candidate + " + operation);
        }
    }

    private void onRootTransition(Operation operation) {
        operation.stateTransition(MetaTypes.OperationStateType.READY);
        blockedToReadyAction(operation);
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
        // normal state transition during execution
        // **BLOCKED**
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.BLOCKED)) {
            inBlockedState(operation, dependencyType, parentState);
        } else if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)) {
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

    private void inBlockedState(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        // BLOCKED->SPECULATIVE
        if (parentState.equals(MetaTypes.OperationStateType.READY) && dependencyType.equals(MetaTypes.DependencyType.SP_LD)) {
            operation.setSpeculativeCandidate();
            if (operation.trySpeculative(false)) {
                blockedToSpeculativeAction(operation);
            }
        } else if (parentState.equals(MetaTypes.OperationStateType.EXECUTED) && dependencyType.equals(MetaTypes.DependencyType.SP_LD)) {
//                operation.setReadyCandidate();
            if (operation.isReadyCandidate()) {
                if (operation.tryReady(false)) {
                    blockedToReadyAction(operation);
                }
            }
        } else if (parentState.equals(MetaTypes.OperationStateType.EXECUTED)) {
            // BLOCKED->READY or BLOCKED->SPECULATIVE
            if (operation.isReadyCandidate()) {
                if (operation.tryReady(false)) {
                    blockedToReadyAction(operation);
                }
            } else if (operation.isSpeculativeCandidate()) {
                if (operation.trySpeculative(false)) {
                    blockedToSpeculativeAction(operation);
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
                getTargetStateManager(descendant).onHeaderStateUpdated(descendant, MetaTypes.OperationStateType.COMMITTED);
            }
        }
        // notify all td children committed
        ArrayDeque<Operation> children = operation.getChildren(MetaTypes.DependencyType.TD);
        for (Operation child : children) {
            getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.COMMITTED);
        }
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        getTargetStateManager(header).onDescendantStateUpdated(header, MetaTypes.OperationStateType.COMMITTABLE);
    }

    private void blockedToReadyAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
//        shortCutListener.onExecutable(operation, true);
        shortCutListener.onExecutable(operation, true, thread_id);
        ArrayDeque<Operation> children = operation.getChildren(MetaTypes.DependencyType.SP_LD);
        // notify the size - 1 number of operations to speculative execute in parallel
        // the last one keeps in blocked state for as a potential future ready candidate.
        int last = children.size() - 1;
        int curIdx = 0;
        for (Operation child : children) {
            if (curIdx != last) {
                getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.SP_LD, MetaTypes.OperationStateType.READY);
            }
            curIdx++;
        }
    }

    private void blockedToSpeculativeAction(Operation operation) {
//        shortCutListener.onExecutable(operation, false);
        shortCutListener.onExecutable(operation, false, thread_id);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        ArrayDeque<Operation> children = operation.getChildren(MetaTypes.DependencyType.SP_LD);
        if (operation.isReadyCandidate()) {
            // if is ready candidate and is executed, elect a new ready candidate
            if (!children.isEmpty()) {
                Operation child = children.getLast();
                getTargetStateManager(child).onReadyParentStateUpdated(child, MetaTypes.DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
            }
        }
        // notify its ld speculative children to countdown, this countdown will only be used for ready candidate.
        // i.e. the read candidate will transit to ready when all its speculative countdown is 0;
        for (Operation child : children) {
            getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
        }

        children = operation.getChildren(MetaTypes.DependencyType.TD);
        for (Operation child : children) {
            getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.EXECUTED);
        }
        children = operation.getChildren(MetaTypes.DependencyType.FD);
        for (Operation child : children) {
            getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }

    }

    private PartitionStateManager getTargetStateManager(Operation child) {
        String operationChainKey = child.getOperationChainKey();
        return Controller.stateToThreadMapping.get(operationChainKey);
    }

    public void initialize(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
        this.shortCutListener = shortCutListener;
    }
}