package scheduler.context;

import scheduler.impl.nonlayered.GSSchedulerWithAbort;
import scheduler.statemanager.OperationChainStateListener;
import scheduler.statemanager.PartitionStateManagerWithAbort;
import scheduler.struct.gs.GSOperationChainWithAbort;
import scheduler.struct.gs.GSOperationWithAbort;

import java.util.concurrent.ConcurrentLinkedDeque;

public class GSTPGContextWithAbort extends AbstractGSTPGContext<GSOperationWithAbort, GSOperationChainWithAbort> {

    public final PartitionStateManagerWithAbort partitionStateManager;


    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public GSTPGContextWithAbort(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManagerWithAbort();
    }

    @Override
    public GSOperationChainWithAbort createTask(String tableName, String pKey, long bid) {
        GSOperationChainWithAbort oc = new GSOperationChainWithAbort(tableName, pKey, bid);
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
