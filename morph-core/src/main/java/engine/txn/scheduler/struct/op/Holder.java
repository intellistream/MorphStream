package engine.txn.scheduler.struct.op;

import util.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>();
}
