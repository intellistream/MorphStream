package scheduler.struct;

import utils.lib.ConcurrentHashMap;

import java.util.Deque;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder<Task extends OperationChain> {
    public ConcurrentHashMap<String, Task> holder_v1 = new ConcurrentHashMap<>(100000);
}
