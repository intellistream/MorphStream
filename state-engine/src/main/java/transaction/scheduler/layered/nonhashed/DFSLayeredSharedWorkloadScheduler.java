package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;

public class DFSLayeredSharedWorkloadScheduler extends LayeredSharedWorkloadScheduler {

    public DFSLayeredSharedWorkloadScheduler(int tp) {
        super(tp);
    }

    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = getOC(threadId, currentLevel[threadId]);
        while (oc == null) {
            if (finishedScheduling(threadId))
                break;
            currentLevel[threadId] += 1;
            oc = getOC(threadId, currentLevel[threadId]);
        }
        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while (oc != null && oc.hasParents()) ; // wait for dependency resolution
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

}
