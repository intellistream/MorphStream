package transaction.scheduler.tpg;
import transaction.scheduler.SchedulerContext;
public class TPGContext extends SchedulerContext {
    private final int thisTaskId;
    public TPGContext(int thisTaskId) {

        this.thisTaskId = thisTaskId;
    }
    @Override
    protected boolean finished(int threadId) {
        return false;
    }
    @Override
    protected void reset() {

    }
}
