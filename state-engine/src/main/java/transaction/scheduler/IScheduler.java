package transaction.scheduler;

public interface IScheduler {
    void INITIALIZE(int threadId);
    void PROCESS(int threadId, long mark_ID);
    void EXPLORE(int threadId);
    boolean FINISHED(int threadId);
    void RESET();
    boolean SubmitRequest(Request request);
    void TxnSubmitBegin(int thread_Id);
    void TxnSubmitFinished(int thread_Id);

}
