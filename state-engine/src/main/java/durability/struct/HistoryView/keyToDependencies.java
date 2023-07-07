package durability.struct.HistoryView;


import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class keyToDependencies implements Serializable {
    public ConcurrentHashMap<String, Node> holder = new ConcurrentHashMap<>();//Each key as a node
    public void addDependencies(String from, String to, long bid, Object v) {
        this.holder.putIfAbsent(from, new Node(from));
        this.holder.get(from).addEdges(to, bid, v);
    }
    public void cleanDependency() {
        for (Node edges : this.holder.values()) {
            edges.clean();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Node> ds : holder.entrySet()) {
            if (ds.getValue().isVisited.get()) {
                sb.append(ds.getKey());
                sb.append(":");
                sb.append(ds.getValue().toString());
                sb.append(";");
            }
        }
        return sb.toString();
    }
}
