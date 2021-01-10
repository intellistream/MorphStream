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

//        Random mRandomGeneratorForAstIds = new Random(123456789);
//        BufferedWriter fileWriter = null;
//        try {
//            File file = new File("C:\\Users\\Aqif\\sesame\\SYNTH_DATA\\testFolder\\dist.txt");
//            if (!file.exists())
//                file.createNewFile();
//
//            fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
//
//            int nu = 10000;
//            int range = 1000;
//            for(int lop = 0; lop< nu; lop++) {
//                int dd = (int)Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian()/2.5)*range)%range;
//                fileWriter.write (dd+"");
//                if(lop<(nu-1))
//                    fileWriter.write(",");
//            }
//            fileWriter.close();
//
//        } catch (IOException e) {
//            System.out.println("An error occurred while storing transactions.");
//            e.printStackTrace();
//        }
//        fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));

    }
}
