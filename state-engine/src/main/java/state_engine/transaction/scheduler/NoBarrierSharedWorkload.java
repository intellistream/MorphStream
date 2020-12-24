package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;

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
            currentDLevelToProcess[threadId]+=1;
            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }
        while(oc!=null && oc.hasDependency()); // wait for dependency resolution
        return oc;
    }

}
