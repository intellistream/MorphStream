package intellistream.morphstream.engine.txn.scheduler.context.og;

import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.statemanager.og.OperationChainStateListener;
import intellistream.morphstream.engine.txn.scheduler.statemanager.og.PartitionStateManagerWithAbort;

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
}
