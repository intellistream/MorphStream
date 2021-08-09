package transaction.scheduler;


public abstract class Scheduler<Context, Task> implements IScheduler<Context> {
    protected abstract void DISTRIBUTE(Task task, Context context);
}
