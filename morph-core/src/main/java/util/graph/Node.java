package util.graph;

public class Node {
    public int id;
    public int weight;

    public Node(int id, int weight) {
        this.id = id;
        this.weight = weight;
    }

    public int getId() {
        return id;
    }

    public int getWeight() {
        return weight;
    }
}
