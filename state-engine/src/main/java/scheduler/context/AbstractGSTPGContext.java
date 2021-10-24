package scheduler.context;

import scheduler.statemanager.OperationChainStateListener;
import scheduler.struct.gs.AbstractGSOperationChain;
import scheduler.struct.gs.GSOperation;

import java.util.ArrayDeque;

public abstract class AbstractGSTPGContext<ExecutionUnit extends GSOperation, SchedulingUnit extends AbstractGSOperationChain<ExecutionUnit>> extends OCSchedulerContext<SchedulingUnit> {

    public ArrayDeque<SchedulingUnit> IsolatedOC;
    public ArrayDeque<SchedulingUnit> OCwithChildren;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public AbstractGSTPGContext(int thisThreadId) {
        super(thisThreadId);
        IsolatedOC = new ArrayDeque<>();
        OCwithChildren = new ArrayDeque<>();
    }

    @Override
    public void reset() {
        super.reset();
        IsolatedOC.clear();
        OCwithChildren.clear();
    }

    @Override
    public SchedulingUnit createTask(String tableName, String pKey, long bid) {
        throw new UnsupportedOperationException("Unsupported.");
    }

    @Override
    public boolean finished() {
        assert scheduledOPs <= totalOsToSchedule;
        return scheduledOPs == totalOsToSchedule;
    }

    public OperationChainStateListener getListener() {
        throw new UnsupportedOperationException();
    }
};
