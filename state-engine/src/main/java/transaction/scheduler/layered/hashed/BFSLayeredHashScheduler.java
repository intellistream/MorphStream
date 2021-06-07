package transaction.scheduler.layered.hashed;

import common.OperationChain;

/**
 * breath-first-search based layered hash scheduler.
 */
public class BFSLayeredHashScheduler extends LayeredHashScheduler {
    public BFSLayeredHashScheduler(int tp) {
        super(tp);

    }
    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = BFSearch(threadId);
        checkFinished(threadId);
        if (oc != null)
            context.scheduledOcsCount[threadId] += 1;
        return oc;// if a null is returned, it means, we are done with level!
    }
}
