package engine.txn.scheduler.struct.recovery;

import util.ConcurrentHashMap;

public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>();
}
