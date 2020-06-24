package sesame.optimization.impl.scaling;

import java.util.HashMap;


public class Parallelism extends HashMap<String, Integer> {
    private static final long serialVersionUID = 3574295926004443115L;

    public int total_num;

    public Parallelism(Parallelism parallelism) {
        for (String topo : parallelism.keySet()) {
            this.put(topo, parallelism.get(topo));
        }
        this.total_num = parallelism.total_num;
    }

    public Parallelism() {

    }

    public void increment(String id, int inc) {
        this.put(id, this.getOrDefault(id, 0) + inc);
        this.total_num += inc;
    }
}
