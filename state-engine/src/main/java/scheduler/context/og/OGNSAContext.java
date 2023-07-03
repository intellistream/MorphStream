package scheduler.context.og;

import scheduler.impl.og.nonstructured.OGNSAScheduler;
import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.statemanager.og.PartitionStateManagerWithAbort;

public class OGNSAContext extends AbstractOGNSContext {

    public final PartitionStateManagerWithAbort partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGNSAContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    public void initialize(OGNSAScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    @Override
    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
};
