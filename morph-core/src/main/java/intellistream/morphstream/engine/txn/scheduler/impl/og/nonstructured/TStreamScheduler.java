package intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured;

import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;

public class TStreamScheduler extends OGNSScheduler {

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public boolean needAbortHandling = false;

    public TStreamScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(OGNSContext context) {
//        tpg.constructTPG(context);
        tpg.Explore(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }

    @Override
    public void REINITIALIZE(OGNSContext context) {
        tpg.ReExplore(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }
}
