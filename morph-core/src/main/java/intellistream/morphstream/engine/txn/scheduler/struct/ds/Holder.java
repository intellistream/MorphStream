package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import java.util.concurrent.ConcurrentHashMap;

public class Holder<Task extends OperationChain>{
    public ConcurrentHashMap<String, Task> holder_v1 = new ConcurrentHashMap<>();//pKey -> operation chain
}
