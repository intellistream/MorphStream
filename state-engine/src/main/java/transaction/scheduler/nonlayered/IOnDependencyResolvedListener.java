package transaction.scheduler.nonlayered;
import common.OperationChain;
public interface IOnDependencyResolvedListener {
    void onParentsResolvedListener(int threadId, OperationChain oc);
}
