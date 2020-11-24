package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception{

        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();
//        Random rand = new Random();
//        FileWriter writer = new FileWriter(new File("C:\\Users\\Aqif\\IdeaProjects\\thesisdata\\data_normal.txt"));
//        int lop=0;
//        HashMap<Integer, Object> ids = new HashMap<>();
//
//        int range = 10 * 16000 * 5;
//
//        while(lop<16000) {
//            int half = range/4;
//            int newVal = range/2 + ((int)(rand.nextGaussian()*(half*1.0f)));
//            if(newVal>0 && newVal<(range)) {
//                if(!ids.containsKey(newVal)) {
//                    ids.put(newVal, null);
//                    writer.write(String.format("%d ", newVal));
//                    lop++;
//                    if(lop%100000==0)
//                        System.out.println(String.format("%d ids generated.", lop));
//                }
//            }
//        }
//        System.out.println(String.format("Done."));
//        writer.close();
    }
}
