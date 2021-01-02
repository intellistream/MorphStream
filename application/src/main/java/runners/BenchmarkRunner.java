package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;
import state_engine.common.OperationChain;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();
//        ConcurrentSkipListMap<Integer, Object> map = new ConcurrentSkipListMap<>();
//        map.put(0, new OperationChain("a1", "a1"));
//        map.put(2, new OperationChain("a3", "a3"));
//        map.put(1, new OperationChain("a2", "a2"));
//        map.put(3, new OperationChain("a4", "a4"));
//        System.out.println(map.headMap(0).lastEntry());

    }

}
