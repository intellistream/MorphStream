package scheduler.statemanager;

import scheduler.context.GSTPGContext;
import scheduler.impl.nonlayered.GSScheduler;
import scheduler.signal.oc.*;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.gs.GSOperationChain;
import scheduler.struct.gs.GSOperation;

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
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(GSOperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(GSOperationChain operationChain) {
        operationChain.isExecuted = true;
        for (GSOperationChain child : operationChain.getFDChildren()) {
            ((GSTPGContext) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
        }
        executableTaskListener.onOCFinalized(operationChain);

    }

    private void ocParentExecutedTransition(GSOperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents()) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    public void initialize(GSScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
