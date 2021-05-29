package transaction.scheduler;
import common.OperationChain;
import java.util.Collection;

/**
 * Author: Aqif Hamid
 * The customized execution scheduler.
 */
public interface IScheduler {
    void submitOperationChains(int threadId, Collection<OperationChain> ocs);
    OperationChain next(int threadId);
    boolean areAllOCsScheduled(int threadId);
    void reSchedule(int threadId, OperationChain oc);
    boolean isReSchedulingEnabled();
    void reset();
}

