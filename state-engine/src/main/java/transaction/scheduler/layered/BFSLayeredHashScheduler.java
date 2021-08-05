package transaction.scheduler.layered;

import transaction.scheduler.layered.struct.OperationChain;
import utils.SOURCE_CONTROL;

/**
 * breath-first-search based layered hash scheduler.
 */
public class BFSLayeredHashScheduler extends LayeredHashScheduler {
    public BFSLayeredHashScheduler(int tp) {
        super(tp);

    }
    @Override
    public OperationChain NEXT(int threadId) {
        OperationChain oc = BFSearch(threadId);
        checkFinished(threadId);
        if (oc != null)
            context.scheduledOcsCount[threadId] += 1;
        return oc;// if a null is returned, it means, we are done with level!
    }

    /**
     * The following are methods to explore global buckets.
     * It has two different ways: DFS and BFS.
     *
     * @param threadId
     * @return
     */

    public OperationChain BFSearch(int threadId) {
        OperationChain oc = Distribute(threadId);
        if (oc != null) {
            return oc;//successfully get the next operation chain of the current level.
        } else {
            if (!Finished(threadId)) {
                while (oc == null) {
                    if (Finished(threadId))
                        break;
                    context.currentLevel[threadId] += 1;//current level is done, process the next level.
                    oc = Distribute(threadId);
                    SOURCE_CONTROL.getInstance().waitForOtherThreads();
                }
            }
        }
        return oc;
    }
}
