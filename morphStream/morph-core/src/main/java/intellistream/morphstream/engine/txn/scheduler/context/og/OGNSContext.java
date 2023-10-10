package intellistream.morphstream.engine.txn.scheduler.context.og;

import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSScheduler;
import intellistream.morphstream.engine.txn.scheduler.statemanager.og.OperationChainStateListener;
import intellistream.morphstream.engine.txn.scheduler.statemanager.og.PartitionStateManager;

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
