package scheduler.struct.og;

import utils.lib.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder<Task extends OperationChain> {
    public ConcurrentHashMap<String, Task> holder_v1 = new ConcurrentHashMap<>();
}
