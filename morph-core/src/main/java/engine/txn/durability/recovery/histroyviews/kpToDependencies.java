package engine.txn.durability.recovery.histroyviews;

import util.ConcurrentHashMap;

public class kpToDependencies {
    public ConcurrentHashMap<String, DependencyEdges> kpToDependencies = new ConcurrentHashMap<>();
    public void addDependency(String from, String to, long bid, Object value) {
        this.kpToDependencies.putIfAbsent(from, new DependencyEdges());
        this.kpToDependencies.get(from).addDependency(to, bid, value);
    }
    public Object inspectDependency(String from, String to, long bid) {
        if (!this.kpToDependencies.containsKey(from))
            return null;
        return this.kpToDependencies.get(from).inspectDependency(to, bid);
    }
}
