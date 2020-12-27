package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;

import java.util.concurrent.ConcurrentLinkedQueue;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();


    }

}
