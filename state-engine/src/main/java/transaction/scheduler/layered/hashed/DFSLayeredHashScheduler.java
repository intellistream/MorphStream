package transaction.scheduler.layered.hashed;

import common.OperationChain;

/**
 * depth-first-search based layered hash scheduler.
 */
public class DFSLayeredHashScheduler extends LayeredHashScheduler {
    public DFSLayeredHashScheduler(int tp) {
        super(tp);
    }
    @Override
    public OperationChain nextOperationChain(int threadId) {
        OperationChain oc = DFSearch(threadId);
        checkFinished(threadId);
        if (oc != null)
            context.scheduledOcsCount[threadId] += 1;
        return oc;// if a null is returned, it means, we are done with level!
    }
}
