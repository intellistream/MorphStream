package transaction.scheduler.layered.nonhashed;

import common.OperationChain;
import profiler.MeasureTools;

public class DFSLayeredRoundRobinScheduler extends LayeredRoundRobinScheduler {

    public DFSLayeredRoundRobinScheduler(int tp) {
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
        while (oc != null && oc.hasParents()) ;
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

}
