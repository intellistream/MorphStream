package scheduler.struct.recovery;

import utils.lib.ConcurrentHashMap;

public class Holder {
    public ConcurrentHashMap<String, OperationChain> holder_v1 = new ConcurrentHashMap<>();
}
