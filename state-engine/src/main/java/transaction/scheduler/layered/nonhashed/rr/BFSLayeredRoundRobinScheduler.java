package transaction.scheduler.layered.nonhashed.rr;

import common.OperationChain;

public class BFSLayeredRoundRobinScheduler extends LayeredRoundRobinScheduler {

    public BFSLayeredRoundRobinScheduler(int tp) {
        super(tp);
    }

    @Override
    public OperationChain NEXT(int threadId) {
        OperationChain oc = BFSearch(threadId);
        return oc;// if a null is returned, it means, we are done with level!
    }
}
