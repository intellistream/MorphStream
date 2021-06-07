package transaction.scheduler;
import common.OperationChain;
import java.util.Collection;

/**
 * Author: Aqif Hamid
 * The customized execution scheduler abstraction.
 */
public interface IScheduler {
    void submitOperationChains(int threadId, Collection<OperationChain> ocs);
    OperationChain nextOperationChain(int threadId);
    boolean finishedScheduling(int threadId);
    void reset();
}

