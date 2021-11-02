package scheduler.statemanager;

import scheduler.context.GSTPGContextWithAbort;
import scheduler.impl.nonlayered.GSSchedulerWithAbort;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.signal.oc.*;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static scheduler.oplevel.struct.MetaTypes.*;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManagerWithAbort implements Runnable, OperationChainStateListener<GSOperationWithAbort, GSOperationChainWithAbort> {
    public final Queue<OperationChainSignal<GSOperationWithAbort, GSOperationChainWithAbort>> ocSignalQueue;
    private GSSchedulerWithAbort.ExecutableTaskListener executableTaskListener;

    public PartitionStateManagerWithAbort() {
        this.ocSignalQueue = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    @Override
    public void onOcRootStart(GSOperationChainWithAbort operationChain) {
        ocSignalQueue.add(new OnRootSignal<>(operationChain));
    }

    @Override
    public void onOcExecuted(GSOperationChainWithAbort operationChain) {
        ocSignalQueue.add(new OnExecutedSignal<>(operationChain));
    }

    @Override
    public void onOcParentExecuted(GSOperationChainWithAbort operationChain, DependencyType dependencyType) {
        ocSignalQueue.add(new OnParentExecutedSignal<>(operationChain, dependencyType));
    }

    public void onHeaderStartAbortHandling(GSOperationChainWithAbort operationChain, GSOperationWithAbort abortedOp) {
        ocSignalQueue.add(new OnHeaderStartAbortHandlingSignal<>(operationChain, abortedOp));
    }

    public void onOcNeedAbortHandling(GSOperationChainWithAbort operationChain, GSOperationWithAbort abortedOp) {
        ocSignalQueue.add(new OnNeedAbortHandlingSignal<>(operationChain, abortedOp));
    }

    public void onOcRollbackAndRedo(GSOperationChainWithAbort operationChain) {
        ocSignalQueue.add(new OnRollbackAndRedoSignal<>(operationChain));
    }

    public void handleStateTransitions() {
        OperationChainSignal<GSOperationWithAbort, GSOperationChainWithAbort> ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            GSOperationChainWithAbort operationChain = ocSignal.getTargetOperationChain();
            if (ocSignal instanceof OnRootSignal) {
                ocRootStartTransition(operationChain);
            } else if (ocSignal instanceof OnExecutedSignal) {
                ocExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnParentExecutedSignal) {
                ocParentExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnNeedAbortHandlingSignal) {
                ocAbortHandlingTransition(operationChain, ((OnNeedAbortHandlingSignal<GSOperationWithAbort, GSOperationChainWithAbort>) ocSignal).getOperation());
            } else if (ocSignal instanceof OnRollbackAndRedoSignal) {
                ocRollbackAndRedoTransition(operationChain);
            } else if (ocSignal instanceof OnHeaderStartAbortHandlingSignal) {
                ocHeaderStartAbortHandlingTransition(operationChain, ((OnHeaderStartAbortHandlingSignal<GSOperationWithAbort, GSOperationChainWithAbort>) ocSignal).getOperation());
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(GSOperationChainWithAbort operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(GSOperationChainWithAbort operationChain) {
        if (!operationChain.needAbortHandling) {
            operationChain.isExecuted = true;
            executableTaskListener.onOCFinalized(operationChain);
            for (GSOperationChainWithAbort child : operationChain.getChildren()) {
                if (child.ocParentsCount.get() > 0) {
                    ((GSTPGContextWithAbort) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
                }
            }
        } else {
            scheduleAbortHandling(operationChain);
        }
    }

    private void scheduleAbortHandling(GSOperationChainWithAbort operationChain) {
        // mark aborted operations and notify OC contains header operation to abort
        // notify children to rollback and redo if it is executed
        for (GSOperationWithAbort failedOp : operationChain.failedOperations) {
            ((GSTPGContextWithAbort) failedOp.getHeader().context).partitionStateManager.onHeaderStartAbortHandling(failedOp.getHeader().getOC(), failedOp.getHeader());
        }
        operationChain.needAbortHandling = false;
        operationChain.failedOperations.clear();
        executableTaskListener.onOCExecutable(operationChain);
    }

    private void ocHeaderStartAbortHandlingTransition(GSOperationChainWithAbort operationChain, GSOperationWithAbort header) {
        if (!header.getOperationState().equals(OperationStateType.ABORTED)) {
            // header might receive multiple abort handling signal, but transaction abort should only happen once.
            header.stateTransition(OperationStateType.ABORTED);
            for (GSOperationWithAbort abortedOp : header.getDescendants()) {
                ((GSTPGContextWithAbort) abortedOp.context).partitionStateManager.onOcNeedAbortHandling(abortedOp.getOC(), abortedOp);
            }
        }
    }


    private void ocAbortHandlingTransition(GSOperationChainWithAbort operationChain, GSOperationWithAbort abortedOp) {
        abortedOp.stateTransition(OperationStateType.ABORTED);
        assert operationChain.getOperations().contains(abortedOp);
        if (!abortedOp.isFailed) {
//            System.out.println(abortedOp);
            for (GSOperationWithAbort operation : operationChain.getOperations()) {
                if (!operation.getOperationState().equals(OperationStateType.ABORTED)) {
                    operation.stateTransition(OperationStateType.BLOCKED);
                }
            }
            if (operationChain.isExecuted) {
                operationChain.isExecuted = false;
                notifyChildrenRollbackAndRedo(operationChain);
                executableTaskListener.onOCRollbacked(operationChain);
                executableTaskListener.onOCExecutable(operationChain);
            }
        }

//        if (!operationChain.hasParents()
//                && !operationChain.context.busyWaitQueue.contains(operationChain)
//                && !operationChain.context.IsolatedOC.contains(operationChain)
//                && !operationChain.context.OCwithChildren.contains(operationChain)) {
//            executableTaskListener.onOCExecutable(operationChain);
//        }
//        if (operationChain.needAbortHandling) {
//            operationChain.failedOperations.remove(abortedOp);
//            if (operationChain.failedOperations.isEmpty()) {
//                operationChain.needAbortHandling = false;
//            }
//        }
    }

    private void ocParentExecutedTransition(GSOperationChainWithAbort operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents() && !operationChain.isExecuted) {
            assert (!operationChain.hasParents()
                    && !operationChain.context.busyWaitQueue.contains(operationChain)
                    && !operationChain.context.IsolatedOC.contains(operationChain)
                    && !operationChain.context.OCwithChildren.contains(operationChain));
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    private void ocRollbackAndRedoTransition(GSOperationChainWithAbort operationChain) {
        for (GSOperationWithAbort operation : operationChain.getOperations()) {
            if (!operation.getOperationState().equals(OperationStateType.ABORTED))
                operation.stateTransition(OperationStateType.BLOCKED);
        }
        if (operationChain.isExecuted) {
            operationChain.isExecuted = false;
            operationChain.rollbackDependency();
            notifyChildrenRollbackAndRedo(operationChain);
            executableTaskListener.onOCRollbacked(operationChain);
        }
    }

    private void notifyChildrenRollbackAndRedo(GSOperationChainWithAbort operationChain) {
        // notify children to rollback and redo
        for (GSOperationChainWithAbort child : operationChain.getChildren()) {
            ((GSTPGContextWithAbort) child.context).partitionStateManager.onOcRollbackAndRedo(child);
        }
    }

    public void initialize(GSSchedulerWithAbort.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
