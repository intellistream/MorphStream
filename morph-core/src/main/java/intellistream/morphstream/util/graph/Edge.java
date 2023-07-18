package intellistream.morphstream.util.graph;

import java.util.concurrent.atomic.AtomicInteger;

public class Edge {
    private final int from;
    private final int to;
    private final AtomicInteger weight;

    public Edge(int from, int to, int weight) {
        this.from = from;
        this.to = to;
        this.weight = new AtomicInteger(weight);
    }

    public int getFrom() {
        return from;
    }

    public int getTo() {
        return to;
    }

    public int getWeight() {
        return weight.get();
    }

    public void addWeight() {
        weight.incrementAndGet();
    }

    public void clean() {
        weight.set(0);
    }
}
