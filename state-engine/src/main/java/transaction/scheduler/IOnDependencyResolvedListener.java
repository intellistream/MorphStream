package transaction.scheduler;
import common.OperationChain;
public interface IOnDependencyResolvedListener {
    void onDependencyResolvedListener(int threadId, OperationChain oc);
}
