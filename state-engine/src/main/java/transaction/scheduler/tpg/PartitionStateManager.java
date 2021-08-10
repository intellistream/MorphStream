package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.*;
import transaction.scheduler.tpg.struct.MetaTypes;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;
import transaction.scheduler.tpg.struct.Operation;
import transaction.scheduler.tpg.struct.OperationChain;
import transaction.scheduler.tpg.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements OperationStateListener, Runnable {
    public final ArrayList<String> partition; //  list of states being responsible for
    public final Queue<OperationSignal> opStateTransitionQueue;
    public final Queue<OperationChainSignal> ocDependencyResolvedQueue;
    private TaskPrecedenceGraph.ShortCutListener shortCutListener;

    public PartitionStateManager() {
        this.opStateTransitionQueue = new ConcurrentLinkedQueue<>();
        this.ocDependencyResolvedQueue = new ConcurrentLinkedQueue<>();
        this.partition = new ArrayList<>();
    }

    @Override
    public void onParentStateUpdated(Operation operation, DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opStateTransitionQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onReadyParentStateUpdated(Operation operation, DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opStateTransitionQueue.add(new OnReadyParentExecutedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onHeaderStateUpdated(Operation operation, MetaTypes.OperationStateType headerState) {
        opStateTransitionQueue.add(new OnHeaderUpdatedSignal(operation, headerState));//
    }

    @Override
    public void onDescendantStateUpdated(Operation operation, MetaTypes.OperationStateType descendantState) {
        opStateTransitionQueue.add(new OnDescendantUpdatedSignal(operation, descendantState));//
    }

    @Override
    public void onProcessed(Operation operation) {
        opStateTransitionQueue.add(new OnProcessedSignal(operation, operation.isFailed));//
    }

    public void onRootStart(Operation head) {
        opStateTransitionQueue.add(new OnRootSignal(head));//
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    public void handleStateTransitions() {
        OperationChainSignal ocSignal = ocDependencyResolvedQueue.poll();
        while (ocSignal != null) {
            OperationChain operationChain = ocSignal.getTargetOperationChain();
            onDependencyResolved(operationChain, ocSignal);
            ocSignal = ocDependencyResolvedQueue.poll();
        }

        OperationSignal opSignal = opStateTransitionQueue.poll();
        while (opSignal != null) {
            Operation operation = opSignal.getTargetOperation();
            if (opSignal instanceof OnProcessedSignal) {
                onProcessedTransition(operation, (OnProcessedSignal) opSignal);
            } else if (opSignal instanceof OnParentUpdatedSignal) {
                onParentStateUpdatedTransition(operation, (OnParentUpdatedSignal) opSignal);
            } else if (opSignal instanceof OnDescendantUpdatedSignal) {
                onDescendantUpdatedTransition(operation, (OnDescendantUpdatedSignal) opSignal);
            } else if (opSignal instanceof OnHeaderUpdatedSignal) {
                onHeaderUpdatedTransition(operation, (OnHeaderUpdatedSignal) opSignal);
            } else {
                throw new UnsupportedOperationException("unknow signal received. " + opSignal);
            }
            opSignal = opStateTransitionQueue.poll();
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
        ArrayDeque<Operation> children = operation.getChildren(DependencyType.TD);
        for (Operation child : children) {
            child.context.partitionStateManager.onParentStateUpdated(child, DependencyType.TD, MetaTypes.OperationStateType.COMMITTED);
        }
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onDescendantStateUpdated(header, MetaTypes.OperationStateType.COMMITTABLE);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        ArrayDeque<Operation> children = operation.getChildren(DependencyType.SP_LD);
        if (operation.isReadyCandidate()) {
            // if is ready candidate and is executed, elect a new ready candidate
            if (!children.isEmpty()) {
                Operation child = children.getLast();
                child.context.partitionStateManager.onReadyParentStateUpdated(child, DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
            }
        }
        // notify its ld speculative children to countdown, this countdown will only be used for ready candidate.
        // i.e. the read candidate will transit to ready when all its speculative countdown is 0;
        for (Operation child : children) {
            child.context.partitionStateManager.onParentStateUpdated(child, DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
        }
    }

    public void onDependencyResolved(OperationChain operationChain, OperationChainSignal ocSignal) {
        DependencyType dependencyType = ocSignal.getDependencyType();
        if (dependencyType != null) {
            operationChain.updateDependencies(dependencyType);
            if (!operationChain.hasParents()) { // functional dependencies is 0, can proceed to be processed
                shortCutListener.onExecutable(operationChain);
            }
        } else {
            shortCutListener.onExecutable(operationChain);
        }
    }

    public void initialize(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
        // 1. set listener
        this.shortCutListener = shortCutListener;
    }
}
