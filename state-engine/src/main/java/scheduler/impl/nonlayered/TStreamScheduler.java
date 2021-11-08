package scheduler.impl.nonlayered;

import scheduler.context.GSTPGContext;
import utils.SOURCE_CONTROL;

public class TStreamScheduler extends GSScheduler {

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public boolean needAbortHandling = false;

    public TStreamScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void INITIALIZE(GSTPGContext context) {
//        tpg.constructTPG(context);
        tpg.tStreamExplore(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }

    @Override
    public void REINITIALIZE(GSTPGContext context) {
        tpg.tStreamReExplore(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }
}
