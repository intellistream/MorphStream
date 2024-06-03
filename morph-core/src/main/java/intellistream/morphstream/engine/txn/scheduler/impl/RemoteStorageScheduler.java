package intellistream.morphstream.engine.txn.scheduler.impl;

import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;


public abstract class RemoteStorageScheduler<Context> implements IScheduler<Context> {

    @Override
    public void INITIALIZE(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void PROCESS(Object o, long mark_ID, int batchID) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void EXPLORE(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean FINISHED(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void RESET(Object o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    @Override
    public void start_evaluation(Object o, long mark_ID, int num_events, int batchID) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void initTPG(int offset) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
