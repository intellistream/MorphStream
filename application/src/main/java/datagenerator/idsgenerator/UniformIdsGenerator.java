package datagenerator.idsgenerator;

import java.util.HashMap;
import java.util.Random;

public class UniformIdsGenerator implements IIdsGenerator {

    private Random mRandomGenerator;
    private HashMap<Integer, Integer> mGeneratedIds;
    private int range;
    private int seed;

    public UniformIdsGenerator(int seed, int range) {
        mRandomGenerator = new Random(seed);
        mGeneratedIds = new HashMap<>();
        this.range = range;
        this.seed = seed;
    }

    @Override
    public int getId() {
        int id = mRandomGenerator.nextInt(range);
        while(mGeneratedIds.containsKey(id))
            id = mRandomGenerator.nextInt(range);
        return id;
    }
}
