package engine.txn.durability.recovery.histroyviews;

import java.util.concurrent.ConcurrentHashMap;

public class Dependencies{
    public ConcurrentHashMap<Long, Object> dependencies = new ConcurrentHashMap<>();
    public void addDependency(long bid, Object v) {
        this.dependencies.put(bid, v);
    }
    public Object inspectDependency(long bid) {
        return dependencies.get(bid);
    }
}