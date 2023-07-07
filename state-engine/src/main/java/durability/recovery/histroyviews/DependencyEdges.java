package durability.recovery.histroyviews;

import java.util.concurrent.ConcurrentHashMap;

public class DependencyEdges {
    public ConcurrentHashMap<String, Dependencies> dependencyEdges = new ConcurrentHashMap<>();//<to, Dependencies>
    public void addDependency(String to, long bid, Object v) {
        this.dependencyEdges.putIfAbsent(to, new Dependencies());
        this.dependencyEdges.get(to).addDependency(bid, v);
    }
    public Object inspectDependency(String to, long bid) {
        if (!this.dependencyEdges.containsKey(to))
            return null;
        return this.dependencyEdges.get(to).inspectDependency(bid);
    }
}
