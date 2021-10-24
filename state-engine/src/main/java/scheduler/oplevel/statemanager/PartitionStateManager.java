package scheduler.oplevel.statemanager;

import profiler.MeasureTools;
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
    public void onOpParentExecuted(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
        opSignalQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onOpNeedAbortHandling(Operation operation, MetaTypes.OperationStateType headerState) {
        opSignalQueue.add(new OnNeedAbortHandlingSignal(operation, headerState));//
    }

    @Override
    public void onHeaderStartAbortHandling(Operation operation, MetaTypes.OperationStateType descendantState) {
        opSignalQueue.add(new OnHeaderStartAbortHandlingSignal(operation, descendantState));//
    }

    @Override
    public void onOpProcessed(Operation operation) {
        opSignalQueue.add(new OnProcessedSignal(operation, operation.isFailed));//
    }

    public void onRootStart(Operation head) {
        opSignalQueue.add(new OnRootSignal(head));//
    }

    @Override
    public void onOpRollbackAndRedo(Operation operation, MetaTypes.DependencyType dependencyType, MetaTypes.OperationStateType parentState) {
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
            } else if (signal instanceof OnNeedAbortHandlingSignal) {
                onHeaderUpdatedTransition(operation, (OnNeedAbortHandlingSignal) signal);
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

    private void onHeaderUpdatedTransition(Operation descendant, OnNeedAbortHandlingSignal signal) {
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
            if (operation.tryReady()) {
                // BLOCKED->READY
                blockedToReadyAction(operation);
            }
        }
    }

    private void onProcessedTransition(Operation operation, OnProcessedSignal signal) {
        if (signal.isFailed()) {
            // transit to aborted
//            operation.stateTransition(OperationStateType.ABORTED);
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
            child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.TD, MetaTypes.OperationStateType.EXECUTED);
        }
        children = operation.getChildren(MetaTypes.DependencyType.FD);
        for (Operation child : children) {
            child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }
        children = operation.getChildren(MetaTypes.DependencyType.LD);
        for (Operation child : children) {
            child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.FD, MetaTypes.OperationStateType.EXECUTED);
        }
        executableTaskListener.onOPFinalized(operation);
    }

    public void initialize(OPGSScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }
}
