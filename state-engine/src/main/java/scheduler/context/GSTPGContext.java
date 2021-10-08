package scheduler.context;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.statemanager.OperationChainStateListener;
import scheduler.statemanager.PartitionStateManager;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;
import scheduler.struct.gs.GSOperationChainWithAbort;

import java.util.ArrayDeque;

public class GSTPGContext
        extends AbstractGSTPGContext<GSOperation, GSOperationChain> {

    public final PartitionStateManager partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public GSTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManager();
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    public GSOperationChain createTask(String tableName, String pKey, long bid) {
        GSOperationChain oc = new GSOperationChain(tableName, pKey, bid);
        oc.setContext(this);
        operationChains.add(oc);
        return oc;
    }

    public void initialize(GSScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    @Override
    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
}
