package scheduler.statemanager.og;

import scheduler.context.og.OGNSAContext;
import scheduler.impl.og.nonstructured.OGNSAScheduler;
import scheduler.signal.oc.*;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.nonstructured.NSAOperationChain;
import scheduler.struct.og.nonstructured.NSAOperation;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static scheduler.struct.op.MetaTypes.*;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManagerWithAbort implements Runnable, OperationChainStateListener<NSAOperation, NSAOperationChain> {
    public final Queue<OperationChainSignal<NSAOperation, NSAOperationChain>> ocSignalQueue;
    private OGNSAScheduler.ExecutableTaskListener executableTaskListener;

    public PartitionStateManagerWithAbort() {
        this.ocSignalQueue = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    @Override
    public void onOcRootStart(NSAOperationChain operationChain) {
        ocSignalQueue.add(new OnRootSignal<>(operationChain));
    }

    @Override
    public void onOcExecuted(NSAOperationChain operationChain) {
        ocSignalQueue.add(new OnExecutedSignal<>(operationChain));
    }

    @Override
    public void onOcParentExecuted(NSAOperationChain operationChain, DependencyType dependencyType) {
        ocSignalQueue.add(new OnParentExecutedSignal<>(operationChain, dependencyType));
    }

    public void onHeaderStartAbortHandling(NSAOperationChain operationChain, NSAOperation abortedOp) {
        ocSignalQueue.add(new OnHeaderStartAbortHandlingSignal<>(operationChain, abortedOp));
    }

    public void onOcNeedAbortHandling(NSAOperationChain operationChain, NSAOperation abortedOp) {
        ocSignalQueue.add(new OnNeedAbortHandlingSignal<>(operationChain, abortedOp));
    }

    public void onOcRollbackAndRedo(NSAOperationChain operationChain) {
        ocSignalQueue.add(new OnRollbackAndRedoSignal<>(operationChain));
    }

    public void handleStateTransitions() {
        OperationChainSignal<NSAOperation, NSAOperationChain> ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            NSAOperationChain operationChain = ocSignal.getTargetOperationChain();
            if (ocSignal instanceof OnRootSignal) {
                ocRootStartTransition(operationChain);
            } else if (ocSignal instanceof OnExecutedSignal) {
                ocExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnParentExecutedSignal) {
                ocParentExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnNeedAbortHandlingSignal) {
                ocAbortHandlingTransition(operationChain, ((OnNeedAbortHandlingSignal<NSAOperation, NSAOperationChain>) ocSignal).getOperation());
            } else if (ocSignal instanceof OnRollbackAndRedoSignal) {
                ocRollbackAndRedoTransition(operationChain);
            } else if (ocSignal instanceof OnHeaderStartAbortHandlingSignal) {
                ocHeaderStartAbortHandlingTransition(operationChain, ((OnHeaderStartAbortHandlingSignal<NSAOperation, NSAOperationChain>) ocSignal).getOperation());
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(NSAOperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(NSAOperationChain operationChain) {
        if (!operationChain.needAbortHandling) {
            operationChain.isExecuted = true;
            executableTaskListener.onOCFinalized(operationChain);
            for (NSAOperationChain child : operationChain.getChildren()) {
                if (child.ocParentsCount.get() > 0) {
                    ((OGNSAContext) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
                }
            }
        } else {
            scheduleAbortHandling(operationChain);
        }
    }

    private void scheduleAbortHandling(NSAOperationChain operationChain) {
        // mark aborted operations and notify OC contains header operation to abort
        // notify children to rollback and redo if it is executed
        for (NSAOperation failedOp : operationChain.failedOperations) {
            ((OGNSAContext) failedOp.getHeader().context).partitionStateManager.onHeaderStartAbortHandling(failedOp.getHeader().getOC(), failedOp.getHeader());
        }
        operationChain.needAbortHandling = false;
        operationChain.failedOperations.clear();
        executableTaskListener.onOCExecutable(operationChain);
    }

    private void ocHeaderStartAbortHandlingTransition(NSAOperationChain operationChain, NSAOperation header) {
        if (!header.getOperationState().equals(OperationStateType.ABORTED)) {
            // header might receive multiple abort handling signal, but transaction abort should only happen once.
            header.stateTransition(OperationStateType.ABORTED);
            for (NSAOperation abortedOp : header.getDescendants()) {
                ((OGNSAContext) abortedOp.context).partitionStateManager.onOcNeedAbortHandling(abortedOp.getOC(), abortedOp);
            }
        }
    }


    private void ocAbortHandlingTransition(NSAOperationChain operationChain, NSAOperation abortedOp) {
        abortedOp.stateTransition(OperationStateType.ABORTED);
        assert operationChain.getOperations().contains(abortedOp);
        if (!abortedOp.isFailed) {
//            System.out.println(abortedOp);
            for (NSAOperation operation : operationChain.getOperations()) {
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

    private void ocParentExecutedTransition(NSAOperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents() && !operationChain.isExecuted) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    private void ocRollbackAndRedoTransition(NSAOperationChain operationChain) {
        for (NSAOperation operation : operationChain.getOperations()) {
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

    private void notifyChildrenRollbackAndRedo(NSAOperationChain operationChain) {
        // notify children to rollback and redo
        for (NSAOperationChain child : operationChain.getChildren()) {
            ((OGNSAContext) child.context).partitionStateManager.onOcRollbackAndRedo(child);
        }
    }

    public void initialize(OGNSAScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
