package transaction.scheduler;
import common.OperationChain;
import java.util.Collection;

/**
 * Author: Aqif Hamid
 * The customized execution scheduler abstraction.
 */
public interface IScheduler {
    void SUBMIT(int threadId, Collection<OperationChain> ocs);
    OperationChain NEXT(int threadId);
    void reset();
}

