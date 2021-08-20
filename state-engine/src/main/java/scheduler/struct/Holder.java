package scheduler.struct;

import utils.lib.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>(100000);
}
