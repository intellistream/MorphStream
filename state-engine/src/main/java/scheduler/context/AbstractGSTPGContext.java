package scheduler.context;

import scheduler.impl.nonlayered.GSScheduler;
import scheduler.statemanager.OperationChainStateListener;
import scheduler.statemanager.OperationStateListener;
import scheduler.statemanager.PartitionStateManager;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.AbstractGSOperationChain;

import java.util.ArrayDeque;

public abstract class AbstractGSTPGContext<ExecutionUnit extends GSOperation, SchedulingUnit extends AbstractGSOperationChain<ExecutionUnit>> extends SchedulerContext<SchedulingUnit> {

    public ArrayDeque<SchedulingUnit> IsolatedOC;
    public ArrayDeque<SchedulingUnit> OCwithChildren;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public AbstractGSTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId);
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
    public SchedulingUnit createTask(String tableName, String pKey) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    @Override
    public boolean finished() {
        return scheduledOPs == totalOsToSchedule;
    }

    public OperationChainStateListener getListener() {
        throw new UnsupportedOperationException();
    }
};
