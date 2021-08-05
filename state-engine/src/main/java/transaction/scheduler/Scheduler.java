package transaction.scheduler;
public abstract class Scheduler<T> implements IScheduler {
    protected abstract void DISTRIBUTE(T task, int threadId);
}
