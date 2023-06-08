package scheduler.statemanager.op;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.impl.op.nonstructured.OPNSAScheduler;
import scheduler.signal.op.*;
import scheduler.struct.op.MetaTypes.DependencyType;
import scheduler.struct.op.MetaTypes.OperationStateType;
import scheduler.struct.op.Operation;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static common.CONTROL.enable_log;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManagerWithAbort implements OperationStateListener, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStateManagerWithAbort.class);

    public final Queue<OperationSignal> opSignalQueue;
    private OPNSAScheduler.ExecutableTaskListener executableTaskListener;


    public PartitionStateManagerWithAbort() {
        this.opSignalQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void onOpParentExecuted(Operation operation, DependencyType dependencyType, OperationStateType parentState) {
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
    public void onOpRollbackAndRedo(Operation operation, DependencyType dependencyType, OperationStateType parentState, OperationStateType prevParentState) {
        opSignalQueue.add(new OnRollbackAndRedoSignal(operation, dependencyType, parentState, prevParentState));
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    public void handleStateTransitions() {
        OperationSignal signal = opSignalQueue.poll();
        while (signal != null) {
            Operation operation = signal.getTargetOperation();
            if (signal instanceof OnProcessedSignal) {
                onProcessedTransition(operation, (OnProcessedSignal) signal);
            } else if (signal instanceof OnParentUpdatedSignal) {
                onParentExecutedTransition(operation, (OnParentUpdatedSignal) signal);
            } else if (signal instanceof OnHeaderStartAbortHandlingSignal) {
                onHeaderStartAbortHandlingTransition(operation);
            } else if (signal instanceof OnNeedAbortHandlingSignal) {
                onAbortHandlingTransition(operation, (OnNeedAbortHandlingSignal) signal);
            } else if (signal instanceof OnRollbackAndRedoSignal) {
                onRollbackAndRedoTransition(operation, (OnRollbackAndRedoSignal) signal);
            } else if (signal instanceof OnRootSignal) {
                onRootTransition(operation);
            }
            signal = opSignalQueue.poll();
        }
    }


    private void onRootTransition(Operation operation) {
        operation.stateTransition(OperationStateType.READY);
        readyAction(operation);
    }

    private void onParentExecutedTransition(Operation operation, OnParentUpdatedSignal signal) {
        DependencyType dependencyType = signal.getType();
        OperationStateType parentState = signal.getState();
        // normal state transition during execution
        // **BLOCKED**
        if (operation.getOperationState().equals(OperationStateType.BLOCKED)) {
            operation.updateDependencies(dependencyType, parentState);
            inBlockedState(operation, dependencyType, parentState);
        }
        // the operaiton might already aborted.
    }

    private void inBlockedState(Operation operation, DependencyType dependencyType, OperationStateType parentState) {
        if (parentState.equals(OperationStateType.EXECUTED) && operation.tryReady()) {
            // BLOCKED->READY
            readyAction(operation);
        }
    }

    private void onProcessedTransition(Operation operation, OnProcessedSignal signal) {
        if (signal.isFailed()) {
            // transit to aborted
            failedAction(operation);
        } else {
            if (operation.getOperationState().equals(OperationStateType.READY)) {
                // transit to executed
                operation.stateTransition(OperationStateType.EXECUTED);
                executedAction(operation);
            }
        }
    }

    private void onHeaderStartAbortHandlingTransition(Operation header) {
        if (!header.getOperationState().equals(OperationStateType.ABORTED)) {
            header.stateTransition(OperationStateType.ABORTED); // ensure to schedule abort only once if receive multiple abort signal from descendants.
            for (Operation descendant : header.getDescendants()) {
                descendant.context.getListener().onOpNeedAbortHandling(descendant, OperationStateType.ABORTED);
            }
        }
    }

    private void onAbortHandlingTransition(Operation descendant, OnNeedAbortHandlingSignal signal) {
        if (enable_log) LOG.debug("failed operation: " + descendant);

        OperationStateType prevOperationState;
        if (!descendant.getOperationState().equals(OperationStateType.EXECUTED)) {
            executableTaskListener.onOPFinalized(descendant);
        }
        prevOperationState = descendant.getOperationState();
        descendant.stateTransition(OperationStateType.ABORTED);
        notifyChildrenRollbackAndRedo(descendant, OperationStateType.ABORTED, prevOperationState);
    }

    private void onRollbackAndRedoTransition(Operation operation, OnRollbackAndRedoSignal signal) {
        DependencyType dependencyType = signal.getType();
        OperationStateType parentState = signal.getState();
        if (parentState.equals(OperationStateType.ABORTED)) {
            // update dependencies and then check to rollback
            // EXECUTED -> READY, must, notify children rollback
            // BLOCKED -> READY, possible, put to ready queue
            // READY -> READY, must, do nothing
            // ABORTED -> ABORTED, do nothing
            if (operation.getOperationState().equals(OperationStateType.EXECUTED)) {
                operation.stateTransition(OperationStateType.READY);
                readyAction(operation);
                executableTaskListener.onOPRollbacked(operation);
                notifyChildrenRollbackAndRedo(operation, OperationStateType.READY, OperationStateType.EXECUTED);
            } else if (operation.getOperationState().equals(OperationStateType.BLOCKED)) {
                if (!signal.getPrevParentState().equals(OperationStateType.EXECUTED)) {
                    operation.updateDependencies(dependencyType, parentState);
                }
                if (operation.tryReady()) {
                    readyAction(operation);
                }
            }
        } else if (parentState.equals(OperationStateType.BLOCKED) || parentState.equals(OperationStateType.READY)) {
            // EXECUTED -> BLOCKED, must
            // READY -> BLOCKED, must
            // BLOCKED -> BLOCKED, must
            // ABORTED -> ABORTED, do nothing
            if (operation.getOperationState().equals(OperationStateType.ABORTED)) {
                return; // do nothing for already aborted operations
            }
            operation.updateDependencies(dependencyType, parentState);
            operation.stateTransition(OperationStateType.BLOCKED);
            if (operation.getOperationState().equals(OperationStateType.EXECUTED)) {
                executableTaskListener.onOPRollbacked(operation);
                // notify its children to rollback, otherwise just rollback its own state
                notifyChildrenRollbackAndRedo(operation, OperationStateType.BLOCKED, OperationStateType.EXECUTED);
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void failedAction(Operation operation) {
        // operation failed, notify header to schedule the abort handling
        // in general, schedule all operation in the same txn to abort and rollback and redo subsequent ops
        operation.getHeader().context.getListener().onHeaderStartAbortHandling(operation.getHeader(), OperationStateType.ABORTED);
    }

    private void readyAction(Operation operation) {
        // notify a number of speculative children to execute in parallel.
        executableTaskListener.onExecutable(operation);
    }

    private void executedAction(Operation operation) {
        // put child to the targeting state manager state transitiion queue.
        notifyChildrenExecuted(operation);
        executableTaskListener.onOPFinalized(operation);
    }

    private void notifyChildrenExecuted(Operation operation) {
        for (Operation child : operation.getChildren(DependencyType.TD)) {
            child.context.getListener().onOpParentExecuted(child, DependencyType.TD, OperationStateType.EXECUTED);
        }
        for (Operation child : operation.getChildren(DependencyType.FD)) {
            child.context.getListener().onOpParentExecuted(child, DependencyType.FD, OperationStateType.EXECUTED);
        }
        for (Operation child : operation.getChildren(DependencyType.LD)) {
            child.context.getListener().onOpParentExecuted(child, DependencyType.LD, OperationStateType.EXECUTED);
        }
    }

    private void notifyChildrenRollbackAndRedo(Operation operation, OperationStateType operationStateType, OperationStateType prevOperationState) {
        for (Operation child : operation.getChildren(DependencyType.TD)) {
            child.context.getListener().onOpRollbackAndRedo(child, DependencyType.TD, operationStateType, prevOperationState);
        }
        for (Operation child : operation.getChildren(DependencyType.FD)) {
            child.context.getListener().onOpRollbackAndRedo(child, DependencyType.FD, operationStateType, prevOperationState);
        }
        for (Operation child : operation.getChildren(DependencyType.LD)) {
            child.context.getListener().onOpRollbackAndRedo(child, DependencyType.LD, operationStateType, prevOperationState);
        }
    }

    public void initialize(OPNSAScheduler.ExecutableTaskListener executableTaskListener) {
        this.executableTaskListener = executableTaskListener;
    }
}
