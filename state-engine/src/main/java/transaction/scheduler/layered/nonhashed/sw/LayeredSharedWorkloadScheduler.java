package transaction.scheduler.layered.nonhashed.sw;

import common.OperationChain;
import transaction.scheduler.layered.nonhashed.LayeredNonHashScheduler;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class LayeredSharedWorkloadScheduler extends LayeredNonHashScheduler<Queue<OperationChain>> {
    SWContext<Queue<OperationChain>> context;
    public LayeredSharedWorkloadScheduler(int tp) {
        context = new SWContext(tp, ConcurrentLinkedQueue::new);
    }

    protected OperationChain Distribute(int threadId) {
        Queue<OperationChain> ocs = context.layeredOCBucketGlobal.get(context.currentLevel[threadId]);
        OperationChain oc = null;
        if (ocs != null)
            oc = ocs.poll(); // TODO: This might be costly, maybe we should stop removing and using a counter or use a synchronized queue?
        return oc;
    }

    @Override
    public boolean Finished(int threadId) {
        return context.finished(threadId);
    }
}
