package scheduler.context;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.statemanager.PartitionStateManager;
import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;
import scheduler.struct.bfs.BFSOperationChain;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class GSTPGContext extends SchedulerContext<GSOperationChain> {

    public final PartitionStateManager partitionStateManager;
    public ConcurrentLinkedDeque<GSOperationChain> IsolatedOC;
    public ConcurrentLinkedDeque<GSOperationChain> OCwithChildren;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public GSTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
        partitionStateManager = new PartitionStateManager();
        IsolatedOC = new ConcurrentLinkedDeque<>();
        OCwithChildren = new ConcurrentLinkedDeque<>();
        requests = new ArrayDeque<>();
    }

    @Override
    protected void reset() {
        IsolatedOC = new ConcurrentLinkedDeque<>();
        OCwithChildren = new ConcurrentLinkedDeque<>();
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
