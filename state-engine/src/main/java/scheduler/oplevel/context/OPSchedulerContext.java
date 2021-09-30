package scheduler.oplevel.context;

import scheduler.context.SchedulerContext;

public abstract class OPSchedulerContext  implements SchedulerContext {
    public int thisThreadId;

    protected abstract boolean finished();

    protected abstract void reset();

    public abstract void UpdateMapping(String key);
}
