package transaction.scheduler.layered.nonhashed.sw;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.layered.nonhashed.LayeredNonHashScheduler;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class LayeredSharedWorkloadScheduler extends LayeredNonHashScheduler<Queue<OperationChain>> {
    SWContext<Queue<OperationChain>> context;
    public LayeredSharedWorkloadScheduler(int tp) {
        context = new SWContext(tp, ConcurrentLinkedQueue::new);
    }

    protected OperationChain getOC(int threadId, int dLevel) {
        Queue<OperationChain> ocs = context.layeredOCBucketGlobal.get(dLevel);
        OperationChain oc = null;
        if (ocs != null)
            oc = ocs.poll(); // TODO: This might be costly, maybe we should stop removing and using a counter or use a synchronized queue?
        return oc;
    }

    @Override
    public boolean finishedScheduling(int threadId) {
        return context.currentLevel[threadId] == context.maxDLevel &&
                context.layeredOCBucketGlobal.get(context.maxDLevel).isEmpty();
    }

    @Override
    public void reset() {
        context.layeredOCBucketGlobal.clear();
        for (int lop = 0; lop < context.currentLevel.length; lop++) {
            context.currentLevel[lop] = 0;
        }
        context.maxDLevel = 0;
    }

}
