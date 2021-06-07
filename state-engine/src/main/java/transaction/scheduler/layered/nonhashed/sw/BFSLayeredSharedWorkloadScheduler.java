package transaction.scheduler.layered.nonhashed.sw;
import common.OperationChain;
public class BFSLayeredSharedWorkloadScheduler extends LayeredSharedWorkloadScheduler {
    public BFSLayeredSharedWorkloadScheduler(int tp) {
        super(tp);
    }
    @Override
    public OperationChain NEXT(int threadId) {
        OperationChain oc = BFSearch(threadId);
        return oc;// if a null is returned, it means, we are done with level!
    }
}
