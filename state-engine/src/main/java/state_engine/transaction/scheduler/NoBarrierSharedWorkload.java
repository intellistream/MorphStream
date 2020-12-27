package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;

public class NoBarrierSharedWorkload extends SharedWorkloadScheduler {

    public NoBarrierSharedWorkload(int tp) {
        super(tp);
    }

    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        while(oc==null) {
            if(areAllOCsScheduled(threadId))
                break;
            currentDLevelToProcess[threadId] += 1;
            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }
        MeasureTools.BEGIN_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        while(oc!=null && oc.hasDependency()); // wait for dependency resolution
        MeasureTools.END_GET_NEXT_THREAD_WAIT_TIME_MEASURE(threadId);
        return oc;
    }

}
