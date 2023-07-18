package engine.txn.scheduler.statemanager.op;

import engine.txn.scheduler.impl.op.nonstructured.OPNSScheduler;
import engine.txn.scheduler.signal.op.*;
import engine.txn.scheduler.struct.op.MetaTypes;
import engine.txn.scheduler.struct.op.MetaTypes.OperationStateType;
import engine.txn.scheduler.struct.op.Operation;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements OperationStateListener, Runnable {
    public final Deque<OperationSignal> opSignalQueue;
    private OPNSScheduler.ExecutableTaskListener executableTaskListener;


    public PartitionStateManager() {
        this.opSignalQueue = new ConcurrentLinkedDeque<>();
    }

    @Override
    public void onOpParentExecuted(Operation operation, MetaTypes.DependencyType dependencyType, OperationStateType parentState) {
        opSignalQueue.add(new OnParentUpdatedSignal(operation, dependencyType, parentState));//
    }

    @Override
    public void onOpNeedAbortHandling(Operation operation, OperationStateType headerState) {
        opSignalQueue.add(new OnNeedAbortHandlingSignal(operation, headerState));//
    }

    @Override
    public void onHeaderStartAbortHandling(Operation operation, OperationStateType descendantState) {
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
    public void onOpRollbackAndRedo(Operation operation, MetaTypes.DependencyType dependencyType, OperationStateType parentState, OperationStateType prevParentState) {
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
            } else if (signal instanceof OnRootSignal) {
                onRootTransition(operation);
            }
            signal = opSignalQueue.poll();
        }
    }


    private void onRootTransition(Operation operation) {
        operation.stateTransition(OperationStateType.READY);
        blockedToReadyAction(operation);
    }

    private void onHeaderUpdatedTransition(OnNeedAbortHandlingSignal signal) {
    }

    private void onParentStateUpdatedTransition(Operation operation, OnParentUpdatedSignal signal) {
        MetaTypes.DependencyType dependencyType = signal.getType();
        OperationStateType parentState = signal.getState();
        operation.updateDependencies(dependencyType, parentState);
        // normal state transition during execution
        // **BLOCKED**
        if (operation.getOperationState().equals(OperationStateType.BLOCKED)) {
            if (operation.tryReady()) {
                // BLOCKED->READY
                blockedToReadyAction(operation);
            }
        }
    }

    private void onProcessedTransition(Operation operation, OnProcessedSignal signal) {
        // transit to executed
        operation.stateTransition(OperationStateType.EXECUTED);
        executedAction(operation);
    }

    private void blockedToReadyAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        executableTaskListener.onExecutable(operation);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.TD)) {
            this.onOpParentExecuted(child, MetaTypes.DependencyType.TD, OperationStateType.EXECUTED);
//            this.opSignalQueue.addFirst(new OnParentUpdatedSignal(child, MetaTypes.DependencyType.TD, OperationStateType.EXECUTED));//
        }
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.LD)) {
            child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.LD, OperationStateType.EXECUTED);
//            this.opSignalQueue.addFirst(new OnParentUpdatedSignal(child, MetaTypes.DependencyType.LD, OperationStateType.EXECUTED));//
        }
        for (Operation child : operation.getChildren(MetaTypes.DependencyType.FD)) {
            child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.FD, OperationStateType.EXECUTED);
//            if (operation.context.thisThreadId != child.context.thisThreadId) {
//                child.context.getListener().onOpParentExecuted(child, MetaTypes.DependencyType.FD, OperationStateType.EXECUTED);
//            } else {
//                this.opSignalQueue.addFirst(new OnParentUpdatedSignal(child, MetaTypes.DependencyType.FD, OperationStateType.EXECUTED));//
//            }
        }
        executableTaskListener.onOPFinalized(operation);
    }

    public void initialize(OPNSScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }
}
