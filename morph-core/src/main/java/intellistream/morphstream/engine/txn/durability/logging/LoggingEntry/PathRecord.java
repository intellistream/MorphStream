package intellistream.morphstream.engine.txn.durability.logging.LoggingEntry;

import intellistream.morphstream.engine.txn.durability.struct.HistoryView.DependencyResult;
import intellistream.morphstream.engine.txn.durability.struct.HistoryView.Node;
import intellistream.morphstream.engine.txn.durability.struct.HistoryView.keyToDependencies;
import intellistream.morphstream.util.graph.Edge;
import intellistream.morphstream.util.graph.Graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class PathRecord implements Serializable {
    public List<Long> abortBids = new ArrayList<>();
    public ConcurrentHashMap<String, List<Integer>> tableToPlacing = new ConcurrentHashMap<>();//<Table, Placing>
    public ConcurrentHashMap<String, keyToDependencies> dependencyEdges = new ConcurrentHashMap<>();//<Table, DependencyEdge>

    public void addAbortBid(long bid) {
        abortBids.add(bid);
    }

    public void addDependencyEdge(String table, String from, String to, long bid, Object value) {
        dependencyEdges.putIfAbsent(table, new keyToDependencies());
        dependencyEdges.get(table).addDependencies(from, to, bid, value);
    }

    public void addNode(String table, String from, int weight) {
        dependencyEdges.putIfAbsent(table, new keyToDependencies());
        dependencyEdges.get(table).holder.putIfAbsent(from, new Node(from));
        dependencyEdges.get(table).holder.get(from).setNodeWeight(weight);
    }

    public void reset() {
        this.abortBids.clear();
        this.tableToPlacing.clear();
        for (keyToDependencies edges : this.dependencyEdges.values()) {
            edges.cleanDependency();
        }
    }

    public void dependencyToGraph(Graph graph, String table) {
        keyToDependencies partitionG = this.dependencyEdges.get(table);
        for (Node node : partitionG.holder.values()) {
            graph.addNode(node.from, node.weight);
            for (Edge edge : node.edges.values()) {
                graph.addEdge(edge);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<Integer>> allocation : this.tableToPlacing.entrySet()) {
            sb.append(allocation.getKey());
            sb.append(":");
            for (int pid : allocation.getValue()) {
                sb.append(pid);
                sb.append(":");
            }
            sb.append(";");
        }
        sb.append(" ");
        for (long bid : abortBids) {
            sb.append(bid).append(";");
        }
        // IOUtils.println("abortBids: " + abortBids.size());
        sb.append(" ");
        int size = 0;
        for (Map.Entry<String, keyToDependencies> logs : this.dependencyEdges.entrySet()) {
            for (Node node : logs.getValue().holder.values()) {
                if (node.isVisited.get()) {
                    for (Vector<DependencyResult> edge : node.dependencyEdges.values()) {
                        size += edge.size();
                    }
                }
            }
            String values = logs.getValue().toString();
            if (values.length() == 0)
                continue;
            sb.append(logs.getKey());
            sb.append(";");
            sb.append(values);
            sb.append(" ");
        }
        //IOUtils.println("size: " + size );
        return sb.toString();
    }
}
