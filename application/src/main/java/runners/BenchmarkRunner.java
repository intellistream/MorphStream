package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;

public class BenchmarkRunner {
    public static void main(String[] args){
        IBenchmark benchmark = new BasicBenchmark(args);
        for(int lop=0; lop<100; lop++)
            benchmark.execute();
    }
}
