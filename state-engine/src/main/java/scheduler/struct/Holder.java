package scheduler.struct;

import utils.lib.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder<OC extends OperationChain> {
    public ConcurrentHashMap<String, OC> holder_v1 = new ConcurrentHashMap<>(100000);
}
