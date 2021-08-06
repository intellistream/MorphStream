package transaction;
import index.high_scale_lib.ConcurrentHashMap;
import transaction.scheduler.layered.struct.OperationChain;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>(100000);
}
