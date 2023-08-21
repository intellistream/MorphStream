package durability.struct.HistoryView;
import common.util.graph.Edge;

import java.io.Serializable;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node implements Serializable {
    public AtomicBoolean isVisited = new AtomicBoolean(false);
    public int from;
    public int weight = 0;
    public ConcurrentHashMap<String, Edge> edges = new ConcurrentHashMap<>();//<to, edges>
    public ConcurrentHashMap<String, Vector<DependencyResult>> dependencyEdges = new ConcurrentHashMap<>();//<to, DependencyResults>

    public Node(String from) {
        this.from = Integer.parseInt(from);
    }

    public void addEdges(String to, long bid, Object result) {
        this.isVisited.compareAndSet(false, true);
        this.edges.putIfAbsent(to, new Edge(from, Integer.parseInt(to), 0));
        this.edges.get(to).addWeight();
        this.dependencyEdges.putIfAbsent(to, new Vector<>());
        this.dependencyEdges.get(to).add(new DependencyResult(bid, result));
    }
    public void setNodeWeight(int weight) {
        this.weight = weight;
    }
    public void clean() {
        for (Edge edge : this.edges.values()) {
            edge.clean();
        }
        for (Vector<DependencyResult> results : this.dependencyEdges.values()) {
            results.clear();
        }
        this.isVisited.compareAndSet(true, false);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Vector<DependencyResult>> des:this.dependencyEdges.entrySet()) {
            if (des.getValue().size() == 0) {
                continue;
            }
            stringBuilder.append(des.getKey());
            stringBuilder.append(",");
            for (DependencyResult result : des.getValue()) {
                stringBuilder.append(result.toString());
                stringBuilder.append(",");
            }
            stringBuilder.append(":");
        }
        return stringBuilder.toString();
    }
}
