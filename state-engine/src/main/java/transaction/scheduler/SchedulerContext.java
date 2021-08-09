package transaction.scheduler;

public abstract class SchedulerContext {
    public int thisThreadId;

    protected abstract boolean finished();

    protected abstract void reset();

    public abstract void UpdateMapping(String key);
}
