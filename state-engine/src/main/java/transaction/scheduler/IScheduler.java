package transaction.scheduler;
import common.OperationChain;
import java.util.Collection;

/**
 * Transaction Schedulers.
 */
public interface IScheduler {

    /**
     * Constructing tasks depends on different <synchronization granularity>.
     * It will decide the number of available independent operation chains at each iteration.
     * It trades off submission overhead and parallelism opportunities.
     * It currently includes: layered, operationchained, and operationed granularities.
     * This function will be called multiple times in order to merge operation chains constructed at different threads during the runtime.
     * @param threadId
     * @param ocs
     */
    void construction(int threadId, Collection<OperationChain> ocs);

    /**
     * Picking up the currently available independent operation chains to process dependents on different <task priority> policies.
     * It currently includes: random pick up and with-dependent-first policies.
     * @param threadId
     * @return the operationchain to process.
     */
    OperationChain pickUp(int threadId);

    /**
     * Distribute the currently pick up operation chains dependents on different <task distribution> policies.
     * It currently includes: RR, hash, work-conserving.
     * @param OC
     * @return
     */
    void distribute(OperationChain OC);

//    boolean finishedScheduling(int threadId);
//    void reSchedule(int threadId, OperationChain oc);
//    boolean isReSchedulingEnabled();
    void reset();
}

