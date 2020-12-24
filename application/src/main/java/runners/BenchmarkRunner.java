package runners;

import benchmark.BasicBenchmark;
import benchmark.IBenchmark;
import state_engine.common.OperationChain;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        IBenchmark benchmark = new BasicBenchmark(args);
        benchmark.execute();
    }

    public static void insertInOrder(int priority, ArrayList<Integer> operationChains) {

        if(operationChains.size() == 0 || priority > operationChains.get(operationChains.size()-1)) {
            operationChains.add(priority);
            return;
        }

        int positionToStartLookingFor = 0;
        int start = 0;
        int end = operationChains.size()-1;
        int center = 0;

        while(true) {
            center = (start+end)/2;
            if(center==start || center == end) {
                positionToStartLookingFor = start;
                break;
            }

            if(priority <= operationChains.get(center))
                end = center;
            else
                start = center;
        }

        while(operationChains.get(positionToStartLookingFor)<priority){
            positionToStartLookingFor++;
        }

        operationChains.add(positionToStartLookingFor, priority);
        System.out.println(String.format("Added %d at %d position.", priority, positionToStartLookingFor));
    }
}
