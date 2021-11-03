package scheduler.impl.nonlayered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.GSTPGContext;
import scheduler.oplevel.struct.MetaTypes;
import scheduler.struct.gs.GSOperation;
import scheduler.struct.gs.GSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

public class TStreamScheduler extends GSScheduler {

    public ExecutableTaskListener executableTaskListener = new ExecutableTaskListener();

    public boolean needAbortHandling = false;

    public TStreamScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    @Override
    public void INITIALIZE(GSTPGContext context) {
//        tpg.constructTPG(context);
        tpg.tStreamExplore(context);
        context.partitionStateManager.initialize(executableTaskListener);
        SOURCE_CONTROL.getInstance().waitForOtherThreads();
    }
}
