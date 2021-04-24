package benchmark.datagenerator.idsgenerator;

import java.util.HashMap;
import java.util.Random;

public class NormalIdsGenerator implements IIdsGenerator {

    private Random mRandomGenerator;
    private HashMap<Integer, Integer> mGeneratedIds;
    private int range;


    public NormalIdsGenerator(int seed, int range) {
        mRandomGenerator = new Random(seed);
        mGeneratedIds = new HashMap<>();
        this.range = range;
    }

    @Override
    public int getId() {
        int id = (int)Math.floor(Math.abs(mRandomGenerator.nextGaussian()/3.5)*range)%range;
        while(mGeneratedIds.containsKey(id))
            id = (int)Math.floor(Math.abs(mRandomGenerator.nextGaussian()/3.5)*range)%range;
        return id;
    }
}
