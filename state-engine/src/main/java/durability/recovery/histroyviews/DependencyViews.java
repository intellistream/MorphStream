package durability.recovery.histroyviews;

import java.util.concurrent.ConcurrentHashMap;

public class DependencyViews {
    public ConcurrentHashMap<String, kpToDependencies> tableToDependencies = new ConcurrentHashMap<>();
    public void addDependencies(String table, String from, String to, long bid, Object v) {
        this.tableToDependencies.putIfAbsent(table, new kpToDependencies());
        this.tableToDependencies.get(table).addDependency(from, to, bid, v);
    }
    public Object inspectView(String table, String from, String to, long bid) {
        if (!this.tableToDependencies.containsKey(table))
            return null;
        return this.tableToDependencies.get(table).inspectDependency(from, to, bid);
    }
}
