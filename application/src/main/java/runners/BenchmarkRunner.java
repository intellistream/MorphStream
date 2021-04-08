package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;
import state_engine.common.OperationChain;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();
    }
}
