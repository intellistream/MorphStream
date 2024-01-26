package intellistream.morphstream.engine.txn.scheduler.impl;

import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.scheduler.Request;

public interface IScheduler<Context> {
    void INITIALIZE(Context context);

    void PROCESS(Context context, long mark_ID, int batchID);

    void EXPLORE(Context context);

    boolean FINISHED(Context context);

    void RESET(Context context);

    boolean SubmitRequest(Context context, Request request);

    void TxnSubmitBegin(Context context);

    void TxnSubmitFinished(Context context, int batchID);

    void AddContext(int thisTaskId, Context context);

    void start_evaluation(Context context, long mark_ID, int num_events, int batchID);

    void initTPG(int offset);

    void setLoggingManager(LoggingManager loggingManager);
}
