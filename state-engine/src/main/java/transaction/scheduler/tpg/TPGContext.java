package transaction.scheduler.tpg;
import transaction.scheduler.SchedulerContext;
public class TPGContext extends SchedulerContext {
    @Override
    protected boolean finished(int threadId) {
        return false;
    }
    @Override
    protected void reset() {

    }
}
