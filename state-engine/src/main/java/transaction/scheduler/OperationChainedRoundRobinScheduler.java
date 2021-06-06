package transaction.scheduler;

import common.OperationChain;
import profiler.MeasureTools;
/**
 * Author: Aqif Hamid
 * Concrete impl of Operation Chained round robin scheduler
 * Share the same submit logic of LayeredRR.
 */
public class OperationChainedRoundRobinScheduler extends LayeredRoundRobinScheduler {

    public OperationChainedRoundRobinScheduler(int tp) {
        super(tp);
    }

    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        while (oc == null) {
            if (finishedScheduling(threadId))
                break;
            currentDLevelToProcess[threadId] += 1;
            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }
        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while (oc != null && oc.hasParents()) ;
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

}
