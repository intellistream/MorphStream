package scheduler.context.og;

import scheduler.impl.og.nonstructured.OGNSScheduler;
import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.statemanager.og.PartitionStateManager;

import java.util.ArrayDeque;

public class OGNSContext extends AbstractOGNSContext {

    public final PartitionStateManager partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGNSContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        requests = new ArrayDeque<>();
        partitionStateManager = new PartitionStateManager();
    }

    public void initialize(OGNSScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
}
