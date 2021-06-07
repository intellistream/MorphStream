package transaction.scheduler.layered.nonhashed.sw;
import common.OperationChain;
public class DFSLayeredSharedWorkloadScheduler extends LayeredSharedWorkloadScheduler {
    public DFSLayeredSharedWorkloadScheduler(int tp) {
        super(tp);
    }
    @Override
    public OperationChain NEXT(int threadId) {
        OperationChain oc = DFSearch(threadId);
        return oc;// if a null is returned, it means, we are done with level!
    }
}
