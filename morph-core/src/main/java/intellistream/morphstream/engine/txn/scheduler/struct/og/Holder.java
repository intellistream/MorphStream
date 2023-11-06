package intellistream.morphstream.engine.txn.scheduler.struct.og;

import java.util.concurrent.ConcurrentHashMap;

/**
 * There shall be $num_op$ Holders.
 */
public class Holder<Task extends OperationChain> {
    public ConcurrentHashMap<String, Task> holder_v1 = new ConcurrentHashMap<>();
}
