package transaction.scheduler.tpg;

import transaction.scheduler.tpg.signal.og.OnExecutedSignal;
import transaction.scheduler.tpg.signal.og.OnParentExecutedSignal;
import transaction.scheduler.tpg.signal.og.OnRootSignal;
import transaction.scheduler.tpg.signal.og.OperationGroupSignal;
import transaction.scheduler.tpg.signal.op.*;
import transaction.scheduler.tpg.struct.*;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
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
            }
            ogSignal = ogSignalQueue.poll();
        }

        OperationSignal opSignal = opSignalQueue.poll();
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
            executedTransition(operation);
            if (operation.tryCommittable()) {
                executedToCommittableAction(operation);
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
    }

    private void executedToCommittableAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        Operation header = operation.getHeader();
        header.context.partitionStateManager.onOpDescendantStateUpdated(header, MetaTypes.OperationStateType.COMMITTABLE);
    }

    private void executedTransition(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        Deque<Operation> children = operation.getChildren(DependencyType.SP_LD);
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
            child.context.partitionStateManager.onOpParentStateUpdated(child, DependencyType.SP_LD, MetaTypes.OperationStateType.EXECUTED);
        }
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

    private void ogRootStartTransition(OperationGroup operationGroup) {
        shortCutListener.onExecutable(operationGroup);
    }

    private void ogExecutedTransition(OperationGroup operationGroup) {
        for (Operation child : operationGroup.getFd_children()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.FD);
        }
        for (Operation child : operationGroup.getLd_children()) {
            child.context.partitionStateManager.onOgParentExecuted(child.getOG(), DependencyType.LD);
        }
        operationGroup.isExecuted = true;
        shortCutListener.onOGFinalized(operationGroup.getOperationGroupId());
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
