package scheduler.impl;

import scheduler.Request;
import scheduler.context.SchedulerContext;
import storage.TableRecord;

public interface IScheduler<Context> {
    void INITIALIZE(Context threadId);

    void PROCESS(Context threadId, long mark_ID);

    void EXPLORE(Context context);

    boolean FINISHED(Context threadId);

    void RESET();

    boolean SubmitRequest(Context context, Request request);

    void TxnSubmitBegin(Context context);

    void TxnSubmitFinished(Context context);

    void AddContext(int thisTaskId, Context context);
}
