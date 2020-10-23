package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;

public class BenchmarkRunner {
    public static void main(String[] args){
        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();
    }
}
