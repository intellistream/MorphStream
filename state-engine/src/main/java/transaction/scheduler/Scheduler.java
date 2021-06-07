package transaction.scheduler;
import common.OperationChain;
public abstract class Scheduler implements IScheduler {

    /**
     * Depends on whether it is layered or nonlayered.
     * Distribute works among threads.
     * <p>
     * If it is layered: It implements different ways to query operation chains from global buckets at certain dependency level.
     * It has three different ways: Hash, RR, and SW.
     *
     * @param threadId
     * @return
     */
    protected abstract OperationChain Distribute(int threadId);

    /**
     * Check if scheduling is finished.
     *
     * @param threadId
     * @return
     */
    protected abstract boolean Finished(int threadId);
}
