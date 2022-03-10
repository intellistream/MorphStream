package scheduler.statemanager.og;

import scheduler.context.og.OGNSContext;
import scheduler.impl.og.nonstructured.OGNSScheduler;
import scheduler.signal.oc.OnExecutedSignal;
import scheduler.signal.oc.OnParentExecutedSignal;
import scheduler.signal.oc.OnRootSignal;
import scheduler.signal.oc.OperationChainSignal;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.nonstructured.NSOperation;
import scheduler.struct.og.nonstructured.NSOperationChain;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManager implements Runnable, OperationChainStateListener<NSOperation, NSOperationChain> {
    public final Queue<OperationChainSignal<NSOperation, NSOperationChain>> ocSignalQueue;
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
    public void onOcRootStart(NSOperationChain operationChain) {
        ocSignalQueue.add(new OnRootSignal<>(operationChain));
    }

    @Override
    public void onOcExecuted(NSOperationChain operationChain) {
        ocSignalQueue.add(new OnExecutedSignal<>(operationChain));
    }

    @Override
    public void onOcParentExecuted(NSOperationChain operationChain, DependencyType dependencyType) {
        ocSignalQueue.add(new OnParentExecutedSignal<>(operationChain, dependencyType));
    }


    public void handleStateTransitions() {
        OperationChainSignal<NSOperation, NSOperationChain> ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            NSOperationChain operationChain = ocSignal.getTargetOperationChain();
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

    private void ocRootStartTransition(NSOperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(NSOperationChain operationChain) {
        operationChain.isExecuted = true;
        for (NSOperationChain child : operationChain.getChildren()) {
            if (child.ocParentsCount.get() > 0) {
                ((OGNSContext) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
            }
        }
        executableTaskListener.onOCFinalized(operationChain);

    }

    private void ocParentExecutedTransition(NSOperationChain operationChain) {
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
