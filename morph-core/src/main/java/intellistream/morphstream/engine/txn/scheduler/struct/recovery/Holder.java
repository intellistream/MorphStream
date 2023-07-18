package intellistream.morphstream.engine.txn.scheduler.struct.recovery;

import java.util.concurrent.ConcurrentHashMap;

public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>();
}
