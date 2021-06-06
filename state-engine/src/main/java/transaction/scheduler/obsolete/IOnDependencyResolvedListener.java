package transaction.scheduler.obsolete;
import common.OperationChain;
public interface IOnDependencyResolvedListener {
    void onDependencyResolvedListener(int threadId, OperationChain oc);
}
