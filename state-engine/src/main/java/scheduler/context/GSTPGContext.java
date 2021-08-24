package scheduler.context;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.statemanager.PartitionStateManager;
import scheduler.struct.gs.GSOperationChain;

import java.util.ArrayDeque;

public class GSTPGContext extends SchedulerContext<GSOperationChain> {

    public final PartitionStateManager partitionStateManager;
    public ArrayDeque<GSOperationChain> IsolatedOC;
    public ArrayDeque<GSOperationChain> OCwithChildren;

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
    protected void reset() {
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
        totalOsToSchedule = 0;
        scheduledOPs = 0;
    }

    @Override
    public GSOperationChain createTask(String tableName, String pKey) {
        return new GSOperationChain(tableName, pKey);
    }

    @Override
    public boolean finished() {
        return scheduledOPs == totalOsToSchedule;
    }

    public void initialize(GSScheduler.ExecutableTaskListener executableTaskListener) {
        partitionStateManager.initialize(executableTaskListener);
    }
};
