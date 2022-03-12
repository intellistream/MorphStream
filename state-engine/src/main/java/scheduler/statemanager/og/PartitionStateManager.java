package scheduler.statemanager.og;

import scheduler.context.og.OGNSContext;
import scheduler.impl.og.nonstructured.OGNSScheduler;
import scheduler.signal.oc.OnExecutedSignal;
import scheduler.signal.oc.OnParentExecutedSignal;
import scheduler.signal.oc.OnRootSignal;
import scheduler.signal.oc.OperationChainSignal;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.OperationChain;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManager implements Runnable, OperationChainStateListener {
    public final Queue<OperationChainSignal> ocSignalQueue;
    private OGNSScheduler.ExecutableTaskListener executableTaskListener;

    public PartitionStateManager() {
        this.ocSignalQueue = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
    }

    @Override
    public void onOcRootStart(OperationChain operationChain) {
        ocSignalQueue.add(new OnRootSignal(operationChain));
    }

    @Override
    public void onOcExecuted(OperationChain operationChain) {
        ocSignalQueue.add(new OnExecutedSignal(operationChain));
    }

    @Override
    public void onOcParentExecuted(OperationChain operationChain, DependencyType dependencyType) {
        ocSignalQueue.add(new OnParentExecutedSignal(operationChain, dependencyType));
    }


    public void handleStateTransitions() {
        OperationChainSignal ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            OperationChain operationChain = ocSignal.getTargetOperationChain();
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

    private void ocRootStartTransition(OperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(OperationChain operationChain) {
        operationChain.isExecuted = true;
        for (OperationChain child : operationChain.getChildren()) {
            if (child.ocParentsCount.get() > 0) {
                ((OGNSContext) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
            }
        }
        executableTaskListener.onOCFinalized(operationChain);

    }

    private void ocParentExecutedTransition(OperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents() && !operationChain.isExecuted) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    public void initialize(OGNSScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
