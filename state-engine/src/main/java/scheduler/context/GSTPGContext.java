package scheduler.context;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.statemanager.OperationChainStateListener;
import scheduler.statemanager.PartitionStateManager;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;

import java.util.ArrayDeque;

public class GSTPGContext
        extends AbstractGSTPGContext<GSOperation, GSOperationChain> {

    public final PartitionStateManager partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public GSTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
        partitionStateManager = new PartitionStateManager();
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    public GSOperationChain createTask(String tableName, String pKey) {
        GSOperationChain oc = new GSOperationChain(tableName, pKey);
        oc.setContext(this);
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
