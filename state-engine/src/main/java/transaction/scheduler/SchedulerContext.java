package transaction.scheduler;
public abstract class SchedulerContext {
    protected abstract boolean finished(int threadId);
    protected abstract void reset();
}
