package util.tools.cacheSim;

import com.vividsolutions.jts.math.Vector2D;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator_Distribution {
    private static double nextTime(double rateParameter) {
        return -Math.log(1.0 - Math.random()) / rateParameter;
    }

    //    static int num_functionTypes;//number of types of functions.
//    private static double total_execution_cycles;//total Brisk.execution cycles.
//    private static double[] num_cycles_per_function;//Brisk.execution cycles per function.
    private static int[] generate_funcTrace(int num_functionTypes, Object[] num_cycles_per_function, int total_execution_cycles) {
        double[] rate = new double[num_functionTypes];
        for (int i = 0; i < num_functionTypes; i++) {
            rate[i] = (double) num_cycles_per_function[i] / total_execution_cycles;
        }
        int[][] pos_ = new int[num_functionTypes][total_execution_cycles];
        int[] new_pos_ = new int[num_functionTypes];
        int[] old_pos_ = new int[num_functionTypes];
        int[] count_ = new int[num_functionTypes];
        for (int i = 0; i < num_functionTypes; i++) {
            count_[i] = (int) (total_execution_cycles * rate[i]);
        }
        LinkedList<Integer> llist = new LinkedList<>();
        int[] event_trace = new int[total_execution_cycles];
        // FileWriter writer;
        //writer = new FileWriter("function_trace.txt", false);
        long start = System.nanoTime();
        for (int i = 0; i < total_execution_cycles; i++) {
            for (int e = 0; e < num_functionTypes; e++) {
                if (rate[e] != 0 && count_[e]-- > 0) {
                    int temp = (int) Math.round(nextTime(rate[e]));
                    if (temp == 0) temp = 1;//Assume no concurrent event
                    new_pos_[e] = temp + old_pos_[e];
                    pos_[e][i] = new_pos_[e];
                    while (llist.contains(new_pos_[e])) {
                        temp = (int) Math.round(nextTime(rate[e]));
                        if (temp == 0) temp = 1;
                        new_pos_[e] = temp + old_pos_[e];
                        pos_[e][i] = new_pos_[e];
                    }
                    llist.add(new_pos_[e]);
                    old_pos_[e] = new_pos_[e];
                    if (pos_[e][i] < total_execution_cycles) {
                        event_trace[pos_[e][i]] = e;
                    }
                }
            }
            // writer.write(String.valueOf(event_trace[i]));
            //  writer.write("\n");
        }
        long end = System.nanoTime();
        System.out.println("Prepare data cost:" + (end - start) / 1000 / 1000 + " milliseconds");
        // writer.flush();
        // writer.clean();
        //System.out.println("Write finished");
        return event_trace;
    }

    private static int getsum(Collection<Integer> l, Map<Integer, Integer> instruction_per_function) {
        int sum = 0;
        for (int i : l) {
            sum += instruction_per_function.get(i);
        }
        return sum;
    }

    private static Vector2D cacheHit(int[] event_trace, Map<Integer, Integer> instruction_per_function, int policy) {
        final int cache_size = 32000;
        int cache_used = 0;
        int compulsory_miss = 0;
        int access_miss = 0;
        // LinkedList<Integer> cached = new LinkedList();
        Map<Integer, Integer> cached = new HashMap<>();
        for (int i : event_trace) {
            if (policy == 1) {
                //update age
                for (int key : cached.keySet()) {
                    cached.put(key, cached.get(key) + 1);
                }
            }
            assert cache_used == getsum(cached.keySet(), instruction_per_function);
            if (!cached.containsKey(i)) {
                if (cache_used + instruction_per_function.get(i) > cache_size) {
                    switch (policy) {
                        case 0: {
                            while (cache_used + instruction_per_function.get(i) > cache_size) {
                                assert (cache_used == getsum(cached.keySet(), instruction_per_function));
                                Random random = new Random();
                                List<Integer> keys = new ArrayList<>(cached.keySet());
                                Integer randomKey = null;
                                try {
                                    randomKey = keys.get(random.nextInt(keys.size()));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                cached.remove(randomKey);
                                cache_used -= instruction_per_function.get(randomKey);
                            }
                            break;
                        }
                        case 1: {
                            while (cache_used + instruction_per_function.get(i) > cache_size) {
                                int lru = 0;
                                int idx = 0;
                                for (int key : cached.keySet()) {
                                    if (cached.get(key) > lru) {
                                        lru = cached.get(key);
                                        idx = key;
                                    }
                                }
                                cached.remove(idx);
                                cache_used -= instruction_per_function.get(idx);
                            }
                            break;
                        }
                    }
                    cache_used += instruction_per_function.get(i);
                    cached.put(i, 0);
                    access_miss++;
                } else {
                    cache_used += instruction_per_function.get(i);
                    cached.put(i, 0);
                    compulsory_miss++;
                }
            } else {
                cached.put(i, 0);//refresh the age to 0.
            }
        }
        return new Vector2D(compulsory_miss, access_miss);
    }

    public static void main(String[] arg) throws IOException {
        Map<Integer, String> function_name = new HashMap<>();
        LinkedList<Double> num_cycles_per_function = new LinkedList<>();
        Map<Integer, Integer> instruction_per_function = new HashMap<>();
        int num_functionTypes = 0;
        int total_execution_cycles = 0;
        String path = null;
        Scanner sc = null;
        int app = 0;
        int policy = 1;//0:random, 1:LRU...
        switch (app) {
            case 0: {//analysis wc
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\wc";
                sc = new Scanner(new File(path + "\\wc.csv"));
                break;
            }
            case 1: {//analysis fd
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\fd";
                sc = new Scanner(new File(path + "\\fd.csv"));
                break;
            }
            case 2: {//analysis lg
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\lg";
                sc = new Scanner(new File(path + "\\lg.csv"));
                break;
            }
            case 3: {//analysis sd
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\sd";
                sc = new Scanner(new File(path + "\\sd.csv"));
                break;
            }
            case 4: {//analysis vs
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\vs";
                sc = new Scanner(new File(path + "\\vs.csv"));
                break;
            }
            case 5: {//analysis tm
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\tm";
                sc = new Scanner(new File(path + "\\tm.csv"));
                break;
            }
            case 6: {//analysis lr
                path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\lr";
                sc = new Scanner(new File(path + "\\lr.csv"));
                break;
            }
        }
        String r;
        String[] r_a;
        while (sc.hasNext()) {
            r = sc.nextLine();
            r_a = r.split(",");
            function_name.put(num_functionTypes, r_a[0].replaceAll("::", "").replaceAll("<", "").replaceAll(">", "").trim());
            num_cycles_per_function.add(new BigDecimal(r_a[1]).doubleValue() / 10E8);
            total_execution_cycles += new BigDecimal(r_a[1]).doubleValue() / 10E8;
            instruction_per_function.put(num_functionTypes, CaculateInst.calculate(path + "\\" + function_name.get(num_functionTypes) + ".csv"));
            num_functionTypes++;
        }
        int[] event_trace = generate_funcTrace(num_functionTypes, num_cycles_per_function.toArray(), total_execution_cycles);
        Vector2D result = cacheHit(event_trace, instruction_per_function, policy);
        System.out.println("compulsory_miss ratio:" + result.getX() / event_trace.length);
        System.out.println("Access_miss ratio:" + result.getY() / event_trace.length);
        System.out.println("Cache Miss:" + (result.getX() + result.getY()) / event_trace.length);
    }
}
