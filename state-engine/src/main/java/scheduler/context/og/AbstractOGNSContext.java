package scheduler.context.og;

import scheduler.statemanager.og.OperationChainStateListener;
import scheduler.struct.og.OperationChain;

import java.util.ArrayDeque;

public abstract class AbstractOGNSContext extends OGSchedulerContext {


    public ArrayDeque<OperationChain> IsolatedOC;
    public ArrayDeque<OperationChain> OCwithChildren;

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
    public boolean finished() {
        assert scheduledOPs <= totalOsToSchedule;
        return scheduledOPs == totalOsToSchedule;
    }

    @Override
    public OperationChain createTask(String tableName, String pKey, long bid) {
        OperationChain oc = new OperationChain(tableName, pKey, bid);
        oc.setContext(this);
//        operationChains.add(oc);
        return oc;
    }

    public OperationChainStateListener getListener() {
        throw new UnsupportedOperationException();
    }
};
