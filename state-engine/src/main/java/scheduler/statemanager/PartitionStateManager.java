package scheduler.statemanager;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.signal.oc.OnExecutedSignal;
import scheduler.signal.oc.OnParentExecutedSignal;
import scheduler.signal.oc.OnRootSignal;
import scheduler.signal.oc.OperationChainSignal;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.OperationChain;
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

    public void handleStateTransitions() {
        OperationChainSignal<GSOperation, GSOperationChain> ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            GSOperationChain operationChain = ocSignal.getTargetOperationChain();
            if (ocSignal instanceof OnRootSignal) {
                ocRootStartTransition(operationChain);
            } else if (ocSignal instanceof OnExecutedSignal) {
                ocExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnParentExecutedSignal) {
                ocParentExecutedTransition(operationChain
                );
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(GSOperationChain operationChain) {
        executableTaskListener.onExecutable(operationChain);
    }


    private void ocExecutedTransition(GSOperationChain operationChain) {
        operationChain.isExecuted = true;
        for (GSOperationChain child : operationChain.getFDChildren()) {
            child.context.partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
        }
//        for (OperationChain<GSOperation> child : operationChain.getFDParents()) { // TODO: update parents children set for priority adjustment
//            ((GSOperationChain) child).context.partitionStateManager.onOcParentExecuted((GSOperationChain) child, DependencyType.FD);
//        }
        executableTaskListener.onOCFinalized(operationChain);
    }

    private void ocParentExecutedTransition(GSOperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents()) {
            executableTaskListener.onExecutable(operationChain); // TODO: to generalize this part
        }
    }

    public void initialize(GSScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
