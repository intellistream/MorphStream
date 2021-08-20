package scheduler.statemanager;

import scheduler.signal.oc.OnExecutedSignal;
import scheduler.signal.oc.OnParentExecutedSignal;
import scheduler.signal.oc.OnRootSignal;
import scheduler.signal.oc.OperationChainSignal;
import scheduler.struct.MetaTypes;
import scheduler.struct.MetaTypes.DependencyType;
import scheduler.struct.MetaTypes.OperationStateType;
import scheduler.struct.Operation;
import scheduler.struct.OperationChain;
import scheduler.struct.TaskPrecedenceGraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Local to every TPGscheduler context.
 */
public class PartitionStateManager implements Runnable, OperationChainStateListener {
    public final ArrayList<String> partition; //  list of states being responsible for
    public final Queue<OperationChainSignal> ocSignalQueue;

    public PartitionStateManager() {
        this.ocSignalQueue = new ConcurrentLinkedQueue<>();
        this.partition = new ArrayList<>();
    }

    public void run() {
        while (!Thread.interrupted()) {
            handleStateTransitions();
        }
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
                ocParentExecutedTransition(operationChain,
                        ((OnParentExecutedSignal) ocSignal).getDependencyType());
            }
            ocSignal = ocSignalQueue.poll();
        }
    }


    /** OC related listener method and transitions **/

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

    private void ocRootStartTransition(OperationChain operationChain) {
    }

    private void ocExecutedTransition(OperationChain operationChain) {
        operationChain.isExecuted = true;
    }

    private void ocParentExecutedTransition(OperationChain operationChain, DependencyType dependencyType) {
    }

    public void initialize() {
    }
}
