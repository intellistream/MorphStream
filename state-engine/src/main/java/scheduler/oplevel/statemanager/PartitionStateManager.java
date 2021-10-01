package scheduler.oplevel.statemanager;

import scheduler.oplevel.impl.tpg.OPGSScheduler;
import scheduler.oplevel.signal.op.*;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.oplevel.struct.Operation;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements OperationStateListener, Runnable {
    public final Queue<OperationSignal> opSignalQueue;
    private OPGSScheduler.ExecutableTaskListener executableTaskListener;


    public PartitionStateManager() {
        this.opSignalQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void onOpParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opSignalQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onOpReadyParentStateUpdated(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
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

    @Override
    public void onOpProcessed(Operation operation) {
        opSignalQueue.add(new OnProcessedSignal(operation, operation.isFailed));//
    }

    public void onRootStart(Operation head) {
        opSignalQueue.add(new OnRootSignal(head));//
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    public void handleStateTransitions() {
//        inTransition.compareAndSet(false, true);
        OperationSignal signal = opSignalQueue.poll();
        while (signal != null) {
            Operation operation = signal.getTargetOperation();
            if (signal instanceof OnProcessedSignal) {
                onProcessedTransition(operation, (OnProcessedSignal) signal);
            } else if (signal instanceof OnParentUpdatedSignal) {
                onParentStateUpdatedTransition(operation, (OnParentUpdatedSignal) signal);
            } else if (signal instanceof OnHeaderUpdatedSignal) {
                onHeaderUpdatedTransition(operation, (OnHeaderUpdatedSignal) signal);
            } else if (signal instanceof OnRootSignal) {
                onRootTransition(operation);
            }
            signal = opSignalQueue.poll();
        }
    }


    private void onRootTransition(Operation operation) {
        operation.stateTransition(MetaTypes.OperationStateType.READY);
        blockedToReadyAction(operation);
    }

    private void onHeaderUpdatedTransition(Operation descendant, OnHeaderUpdatedSignal signal) {
        MetaTypes.OperationStateType headerState = signal.getState();
        // TODO: abort handling
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
        // TODO: abort handling
    }

    private void inBlockedState(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        if (parentState.equals(MetaTypes.OperationStateType.EXECUTED) && operation.tryReady(false)) {
            // BLOCKED->READY
            blockedToReadyAction(operation);
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
        }
    }

    private void blockedToReadyAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        executableTaskListener.onExecutable(operation);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        Queue<Operation> children = operation.getChildren(MetaTypes.DependencyType.TD);
        for (Operation child : children) {
//            getTargetStateManager(child).onParentStateUpdated(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.EXECUTED);
            child.context.getListener().onOpParentStateUpdated(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.EXECUTED);
        }
        children = operation.getChildren(MetaTypes.DependencyType.FD);
        for (Operation child : children) {
            child.context.getListener().onOpParentStateUpdated(child, MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }
        children = operation.getChildren(MetaTypes.DependencyType.LD);
        for (Operation child : children) {
            child.context.getListener().onOpParentStateUpdated(child, MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }
        executableTaskListener.onOPFinalized(operation);
    }

    public void initialize(OPGSScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }
}
