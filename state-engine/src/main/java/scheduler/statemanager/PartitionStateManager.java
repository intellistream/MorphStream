package scheduler.statemanager;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.signal.oc.*;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManager implements Runnable, OperationChainStateListener<GSOperation, GSOperationChain> {
    public final Queue<OperationChainSignal<GSOperation, GSOperationChain>> ocSignalQueue;
    private GSScheduler.ExecutableTaskListener executableTaskListener;

    public PartitionStateManager() {
        this.ocSignalQueue = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    @Override
    public void onOcRootStart(GSOperationChain operationChain) {
        ocSignalQueue.add(new OnRootSignal<>(operationChain));
    }

    @Override
    public void onOcExecuted(GSOperationChain operationChain) {
        ocSignalQueue.add(new OnExecutedSignal<>(operationChain));
    }

    @Override
    public void onOcParentExecuted(GSOperationChain operationChain, DependencyType dependencyType) {
        ocSignalQueue.add(new OnParentExecutedSignal<>(operationChain, dependencyType));
    }

    public void onOcNeedAbortHandling(GSOperationChain operationChain, GSOperation abortedOp) {
        ocSignalQueue.add(new OnNeedAbortHandlingSignal<>(operationChain, abortedOp));
    }

    public void onOcRollbackAndRedo(GSOperationChain operationChain) {
        ocSignalQueue.add(new OnRollbackAndRedoSignal<>(operationChain));
    }

    public void handleStateTransitions() {
        OperationChainSignal<GSOperation, GSOperationChain> ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            GSOperationChain operationChain = ocSignal.getTargetOperationChain();
            if (ocSignal instanceof OnRootSignal) {
                ocRootStartTransition(operationChain);
            } else if (ocSignal instanceof OnExecutedSignal) {
                ocExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnParentExecutedSignal) {
                ocParentExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnNeedAbortHandlingSignal) {
                ocAbortHandlingTransition(operationChain, ((OnNeedAbortHandlingSignal<GSOperation, GSOperationChain>) ocSignal).getOperation());
            } else if (ocSignal instanceof OnRollbackAndRedoSignal) {
                ocRollbackAndRedoTransition(operationChain);
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(GSOperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(GSOperationChain operationChain) {
        if (!operationChain.needAbortHandling) {
            operationChain.isExecuted = true;
            for (GSOperationChain child : operationChain.getFDChildren()) {
                child.context.partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
            }
            executableTaskListener.onOCFinalized(operationChain);
        } else {
            scheduleAbortHandling(operationChain);
        }
    }

    private void scheduleAbortHandling(GSOperationChain operationChain) {
        // mark aborted operations and notify OC contains header operation to abort
        // notify children to rollback and redo if it is executed
        for (GSOperation failedOp : operationChain.failedOperations) {
            for (GSOperation abortedOp : failedOp.getHeader().getDescendants()) {
                abortedOp.context.partitionStateManager.onOcNeedAbortHandling(abortedOp.getOC(), abortedOp);
            }
        }
        operationChain.needAbortHandling = false;
        operationChain.failedOperations.clear();
    }

    private void ocAbortHandlingTransition(GSOperationChain operationChain, GSOperation abortedOp) {
        abortedOp.aborted = true;
        if (operationChain.isExecuted) {
            operationChain.isExecuted = false;
            notifyChildrenRollbackAndRedo(operationChain);
            executableTaskListener.onOCRollbacked(operationChain);
        }
        if (!operationChain.hasParents()) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    private void ocParentExecutedTransition(GSOperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents()) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    private void ocRollbackAndRedoTransition(GSOperationChain operationChain) {
        if (operationChain.isExecuted) {
            operationChain.isExecuted = false;
            operationChain.rollbackDependency();
            notifyChildrenRollbackAndRedo(operationChain);
            executableTaskListener.onOCRollbacked(operationChain);
        }
    }

    private void notifyChildrenRollbackAndRedo(GSOperationChain operationChain) {
        // notify children to rollback and redo
        for (GSOperationChain child : operationChain.getFDChildren()) {
            child.context.partitionStateManager.onOcRollbackAndRedo(child);
        }
    }

    public void initialize(GSScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
