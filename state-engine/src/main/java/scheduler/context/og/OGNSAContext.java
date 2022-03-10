package scheduler.context.og;

import scheduler.impl.og.nonstructured.OGNSAScheduler;
import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.statemanager.og.PartitionStateManagerWithAbort;
import scheduler.struct.og.nonstructured.NSAOperationChain;
import scheduler.struct.og.nonstructured.NSAOperation;

public class OGNSAContext extends AbstractOGNSContext<NSAOperation, NSAOperationChain> {

    public final PartitionStateManagerWithAbort partitionStateManager;


    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGNSAContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    @Override
    public NSAOperationChain createTask(String tableName, String pKey, long bid) {
        NSAOperationChain oc = new NSAOperationChain(tableName, pKey, bid);
        oc.setContext(this);
//        if (!operationChainsLeft.contains(oc))
//        operationChains.add(oc);
        return oc;
    }

    public void initialize(OGNSAScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    @Override
    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
};
