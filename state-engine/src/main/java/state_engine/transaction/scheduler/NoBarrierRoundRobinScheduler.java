package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import state_engine.profiler.MeasureTools;
import state_engine.utils.SOURCE_CONTROL;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NoBarrierRoundRobinScheduler extends RoundRobinScheduler {

    public NoBarrierRoundRobinScheduler(int tp) {
        super(tp);
    }

    @Override
    public OperationChain next(int threadId) {
        OperationChain oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        while(oc==null) {
            if(areAllOCsScheduled(threadId))
                break;
            currentDLevelToProcess[threadId] += 1;
            indexOfNextOCToProcess[threadId] = threadId;
            oc = getOcForThreadAndDLevel(threadId, currentDLevelToProcess[threadId]);
        }
        while(oc!=null && oc.hasDependency());
        return oc;
    }

}
