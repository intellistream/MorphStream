package transaction.scheduler;

public interface IScheduler {
    void INITIALIZE(int threadId);
    void PROCESS(int threadId, long mark_ID);
    void EXPLORE(int threadId);
    void RESET();
    boolean isFinished(int threadId);
}
