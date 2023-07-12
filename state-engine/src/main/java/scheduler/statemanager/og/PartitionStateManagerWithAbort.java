package scheduler.statemanager.og;

import durability.logging.LoggingEntry.PathRecord;
import profiler.MeasureTools;
import scheduler.context.og.OGNSAContext;
import scheduler.impl.og.nonstructured.OGNSAScheduler;
import scheduler.signal.oc.*;
import scheduler.struct.og.MetaTypes.DependencyType;
import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static scheduler.struct.op.MetaTypes.*;

/**
 * Local to every TPGScheduler context.
 */
public class PartitionStateManagerWithAbort implements Runnable, OperationChainStateListener {
    public final Queue<OperationChainSignal> ocSignalQueue;
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

    public void onHeaderStartAbortHandling(OperationChain operationChain, Operation abortedOp) {
        ocSignalQueue.add(new OnHeaderStartAbortHandlingSignal(operationChain, abortedOp));
    }

    public void onOcNeedAbortHandling(OperationChain operationChain, Operation abortedOp) {
        ocSignalQueue.add(new OnNeedAbortHandlingSignal(operationChain, abortedOp));
    }

    public void onOcRollbackAndRedo(OperationChain operationChain) {
        ocSignalQueue.add(new OnRollbackAndRedoSignal(operationChain));
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
            } else if (ocSignal instanceof OnNeedAbortHandlingSignal) {
                ocAbortHandlingTransition(operationChain, ((OnNeedAbortHandlingSignal) ocSignal).getOperation());
            } else if (ocSignal instanceof OnRollbackAndRedoSignal) {
                ocRollbackAndRedoTransition(operationChain);
            } else if (ocSignal instanceof OnHeaderStartAbortHandlingSignal) {
                ocHeaderStartAbortHandlingTransition(operationChain, ((OnHeaderStartAbortHandlingSignal) ocSignal).getOperation());
            }
            ocSignal = ocSignalQueue.poll();
        }
    }
    public void handleStateTransitionsWithAbortTracking(PathRecord pathRecord) {
        OperationChainSignal ocSignal = ocSignalQueue.poll();
        while (ocSignal != null) {
            OperationChain operationChain = ocSignal.getTargetOperationChain();
            if (ocSignal instanceof OnRootSignal) {
                ocRootStartTransition(operationChain);
            } else if (ocSignal instanceof OnExecutedSignal) {
                ocExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnParentExecutedSignal) {
                ocParentExecutedTransition(operationChain);
            } else if (ocSignal instanceof OnNeedAbortHandlingSignal) {
                MeasureTools.BEGIN_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
                ocAbortHandlingTransition(operationChain, ((OnNeedAbortHandlingSignal) ocSignal).getOperation());
                MeasureTools.END_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
            } else if (ocSignal instanceof OnRollbackAndRedoSignal) {
                MeasureTools.BEGIN_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
                ocRollbackAndRedoTransition(operationChain);
                MeasureTools.END_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
            } else if (ocSignal instanceof OnHeaderStartAbortHandlingSignal) {
                MeasureTools.BEGIN_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
                ocHeaderStartAbortHandlingTransition(operationChain, ((OnHeaderStartAbortHandlingSignal) ocSignal).getOperation());
                MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operationChain.context.thisThreadId);
                pathRecord.addAbortBid(((OnHeaderStartAbortHandlingSignal) ocSignal).getOperation().bid);
                MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operationChain.context.thisThreadId);
                MeasureTools.END_SCHEDULE_ABORT_TIME_MEASURE(operationChain.context.thisThreadId);
            }
            ocSignal = ocSignalQueue.poll();
        }
    }

    private void ocRootStartTransition(OperationChain operationChain) {
        executableTaskListener.onOCExecutable(operationChain);
    }


    private void ocExecutedTransition(OperationChain operationChain) {
        if (!operationChain.needAbortHandling) {
            operationChain.isExecuted = true;
            executableTaskListener.onOCFinalized(operationChain);
            for (OperationChain child : operationChain.getChildren()) {
                if (child.ocParentsCount.get() > 0) {
                    ((OGNSAContext) child.context).partitionStateManager.onOcParentExecuted(child, DependencyType.FD);
                }
            }
        } else {
            scheduleAbortHandling(operationChain);
        }
    }

    private void scheduleAbortHandling(OperationChain operationChain) {
        // mark aborted operations and notify OC contains header operation to abort
        // notify children to rollback and redo if it is executed
        for (Operation failedOp : operationChain.failedOperations) {
            ((OGNSAContext) failedOp.getHeader().context).partitionStateManager.onHeaderStartAbortHandling(failedOp.getHeader().getOC(), failedOp.getHeader());
        }
        operationChain.needAbortHandling = false;
        operationChain.failedOperations.clear();
        executableTaskListener.onOCExecutable(operationChain);
    }

    private void ocHeaderStartAbortHandlingTransition(OperationChain operationChain, Operation header) {
        if (!header.getOperationState().equals(OperationStateType.ABORTED)) {
            // header might receive multiple abort handling signal, but transaction abort should only happen once.
            header.stateTransition(OperationStateType.ABORTED);
            for (Operation abortedOp : header.getDescendants()) {
                ((OGNSAContext) abortedOp.context).partitionStateManager.onOcNeedAbortHandling(abortedOp.getOC(), abortedOp);
            }
        }
    }


    private void ocAbortHandlingTransition(OperationChain operationChain, Operation abortedOp) {
        abortedOp.stateTransition(OperationStateType.ABORTED);
        assert operationChain.getOperations().contains(abortedOp);
        if (!abortedOp.isFailed) {
//            System.out.println(abortedOp);
            for (Operation operation : operationChain.getOperations()) {
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

    private void ocParentExecutedTransition(OperationChain operationChain) {
        operationChain.updateDependency();
        if (!operationChain.hasParents() && !operationChain.isExecuted) {
            executableTaskListener.onOCExecutable(operationChain);
        }
    }

    private void ocRollbackAndRedoTransition(OperationChain operationChain) {
        for (Operation operation : operationChain.getOperations()) {
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

    private void notifyChildrenRollbackAndRedo(OperationChain operationChain) {
        // notify children to rollback and redo
        for (OperationChain child : operationChain.getChildren()) {
            ((OGNSAContext) child.context).partitionStateManager.onOcRollbackAndRedo(child);
        }
    }

    public void initialize(OGNSAScheduler.ExecutableTaskListener executableTaskListener) {
        // 1. set listener
        this.executableTaskListener = executableTaskListener;
    }
}
