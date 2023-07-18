package engine.txn.scheduler.struct.og;

import util.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder<Task extends OperationChain> {
    public ConcurrentHashMap<String, Task> holder_v1 = new ConcurrentHashMap<>();
}
