package transaction.scheduler.layered.hashed;

import common.OperationChain;
import profiler.MeasureTools;
import transaction.scheduler.layered.hashed.LayeredHashScheduler;

/**
 * depth-first-search based layered hash scheduler.
 */
public class DFSLayeredHashScheduler extends LayeredHashScheduler {

    public DFSLayeredHashScheduler(int tp) {
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
        while (oc != null && oc.hasParents()) ; // Busy-Wait for dependency resolution
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);

        if (oc != null)
            scheduledOcsCount[threadId] += 1;
        return oc;
    }

}
