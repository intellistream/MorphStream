package scheduler.impl;

import durability.logging.LoggingStrategy.LoggingManager;
import scheduler.Request;

public interface IScheduler<Context> {
    void INITIALIZE(Context threadId);

    void PROCESS(Context threadId, long mark_ID);

    void EXPLORE(Context context);

    boolean FINISHED(Context threadId);

    void RESET(Context context);

    boolean SubmitRequest(Context context, Request request);

    void TxnSubmitBegin(Context context);

    void TxnSubmitFinished(Context context);

    void AddContext(int thisTaskId, Context context);

    void start_evaluation(Context context, long mark_ID, int num_events);

    void initTPG(int offset);
    void setLoggingManager(LoggingManager loggingManager);
}
