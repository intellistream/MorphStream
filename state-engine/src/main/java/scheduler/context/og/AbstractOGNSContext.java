package scheduler.context.og;

import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.struct.og.nonstructured.AbstractNSOperationChain;
import scheduler.struct.og.nonstructured.NSOperation;

import java.util.ArrayDeque;

public abstract class AbstractOGNSContext<ExecutionUnit extends NSOperation, SchedulingUnit extends AbstractNSOperationChain<ExecutionUnit>> extends OGSchedulerContext<SchedulingUnit> {

    public ArrayDeque<SchedulingUnit> IsolatedOC;
    public ArrayDeque<SchedulingUnit> OCwithChildren;

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public AbstractOGNSContext(int thisThreadId) {
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
    public void redo() {
        super.redo();
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
