package scheduler.context;

import scheduler.impl.nonlayered.GSSchedulerWithAbort;
import scheduler.statemanager.OperationChainStateListener;
import scheduler.statemanager.PartitionStateManagerWithAbort;
import scheduler.struct.gs.GSOperationChain;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;

import java.util.ArrayDeque;

public class GSTPGContextWithAbort extends AbstractGSTPGContext<GSOperationWithAbort, GSOperationChainWithAbort> {

    public final PartitionStateManagerWithAbort partitionStateManager;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public GSTPGContextWithAbort(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    @Override
    public GSOperationChainWithAbort createTask(String tableName, String pKey) {
        GSOperationChainWithAbort oc = new GSOperationChainWithAbort(tableName, pKey);
        oc.setContext(this);
        return oc;
    }

    public void initialize(GSSchedulerWithAbort.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }

    @Override
    public OperationChainStateListener getListener() {
        return partitionStateManager;
    }
};
