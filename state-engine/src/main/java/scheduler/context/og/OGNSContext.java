package scheduler.context.og;

import scheduler.impl.og.nonstructured.OGNSScheduler;
import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.statemanager.og.PartitionStateManager;
import scheduler.struct.og.nonstructured.NSOperation;
import scheduler.struct.og.nonstructured.NSOperationChain;

import java.util.ArrayDeque;

public class OGNSContext
        extends AbstractOGNSContext<NSOperation, NSOperationChain> {

    public final PartitionStateManager partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGNSContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManager();
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    public NSOperationChain createTask(String tableName, String pKey, long bid) {
        NSOperationChain oc = new NSOperationChain(tableName, pKey, bid);
        oc.setContext(this);
//        operationChains.add(oc);
        return oc;
    }

    public void initialize(OGNSScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    @Override
    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
}
