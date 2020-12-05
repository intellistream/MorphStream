package state_engine.transaction.scheduler;

import state_engine.common.OperationChain;
import java.util.Collection;

public interface IScheduler {
    void submitOcs(int threadId, Collection<OperationChain> ocs);
    OperationChain next(int threadId);
    boolean areAllOCsScheduled(int threadId);
    void reSchedule(int threadId, OperationChain oc);
    boolean isReSchedulingEnabled();
    void reset();
}

