package util.graph;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class Graph {
    private int nodeSize;
    private Vector<Edge> edges = new Vector<>();
    private int[] nodeWeights;
    private GraphPartitioner graphPartitioner;
    private ConcurrentHashMap<Integer, Integer> keyToPartition = new ConcurrentHashMap<>();
    public Graph(int nodeSize, int partitionCount) {
        this.nodeSize = nodeSize;
        this.nodeWeights = new int[nodeSize];
        this.graphPartitioner = new GraphPartitioner(nodeSize, nodeWeights, edges, partitionCount);
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public void addNode(int id, int weight) {
        nodeWeights[id] = weight;
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
    }
    public void addEdge(int from, int to, int weight) {
        edges.add(new Edge(from, to, weight));
    }

    public int[] getNodeWeights() {
        return nodeWeights;
    }

    public int getNodeSize() {
        return nodeSize;
    }
    public void clean() {
        //Do not impact the result of getPartitions
        edges.clear();
        this.keyToPartition.clear();
    }
    public void partition(int max_itr) {
        graphPartitioner.run(max_itr);
        List<List<Integer>> partitions = getPartitions();
        for (int i = 0; i < partitions.size(); i ++) {
            for (int key : partitions.get(i)) {
                this.keyToPartition.put(key, i);
            }
        }
    }
    public boolean isDifferentPartition(int from, int to) {
        if (!this.keyToPartition.containsKey(from))
            return true;
        int fromPartitionId = this.keyToPartition.get(from);
        int toPartitionId = this.keyToPartition.get(to);
        return fromPartitionId != toPartitionId;
    }

    public List<List<Integer>> getPartitions() {
        return graphPartitioner.getPartitions();
    }
}
