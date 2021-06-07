package transaction.scheduler.layered.nonhashed.rr;

import common.OperationChain;

public class DFSLayeredRoundRobinScheduler extends LayeredRoundRobinScheduler {

    public DFSLayeredRoundRobinScheduler(int tp) {
        super(tp);
    }

    @Override
    public OperationChain NEXT(int threadId) {
        OperationChain oc = DFSearch(threadId);
        return oc;// if a null is returned, it means, we are done with level!
    }
}
