package application.tools.cacheSim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator_PI_callflow {
    static String path = "C:\\Users\\szhang026\\Documents\\compatibility-app\\log\\log";

    private static double nextTime(double rateParameter) {
        return -Math.log(1.0 - Math.random()) / rateParameter;
    }

//    private static int getsum(Collection<String> l, Map<String, Integer> instruction_per_function) {
//        int sum = 0;
//        for (String i : l) {
//            sum += instruction_per_function.get(i);
//        }
//        return sum;
//    }

//    private static Vector2D cacheHit(LinkedList<record> event_trace, int policy) throws IOException {
//        final int cache_size = 32000;
//        int cache_used = 0;
//        int compulsory_miss = 0;
//        int access_miss = 0;
//        // LinkedList<Integer> cached = new LinkedList();
//        Map<String, Integer> cached = new HashMap<>();//<function name, age>
//        for (record i : event_trace) {
//            if (policy == 1) {
//                //update age
//                for (String key : cached.keySet()) {
//                    cached.put(key, cached.get(key) + 1);
//                }
//            }
//          // assert cache_used == getsum(cached.keySet(), instruction_per_function);
//            if (!cached.containsKey(i.NAME)) {
//                if (cache_used + i.PTT_INSTS > cache_size) {
//                    switch (policy) {
//                        case 0: {
//                            while (cache_used + i.PTT_INSTS > cache_size) {
//                               // assert (cache_used == getsum(cached.keySet(), instruction_per_function));
//                                Random random = new Random();
//                                List<String> keys = new ArrayList(cached.keySet());
//                                String randomKey = null;
//                                try {
//                                    randomKey = keys.get(random.nextInt(keys.size()));
//                                } catch (Exception e) {
//                                    e.printStackTrace();
//                                }
//                                cached.remove(randomKey);
//                                cache_used -= instruction_per_function.get(randomKey);
//                            }
//                            break;
//                        }
//                        case 1: {
//                            while (cache_used + instruction_per_function.get(i) > cache_size) {
//                                int lru = 0;
//                                String idx = null;
//                                for (String key : cached.keySet()) {
//                                    if (cached.get(key) > lru) {
//                                        lru = cached.get(key);
//                                        idx = key;
//                                    }
//                                }
//                                cached.remove(idx);
//                                cache_used -= instruction_per_function.get(idx);
//                            }
//                            break;
//                        }
//                    }
//                    cache_used += instruction_per_function.get(i);
//                    cached.put(i, 0);
//                    access_miss++;
//                } else {
//                    cache_used += instruction_per_function.get(i);
//                    cached.put(i, 0);
//                    compulsory_miss++;
//                }
//            } else {
//                cached.put(i, 0);//refresh the age to 0.
//            }
//        }
//        return new Vector2D(compulsory_miss, access_miss);
//    }

    private static LinkedList<record> clean_InTrace_results(int app) throws IOException {
        FileWriter writer = null;

        Scanner sc = null;
        switch (app) {
            case 4: {//analysis wc

                sc = new Scanner(new File(path + "\\4\\wc.trace"));
                writer = new FileWriter(path + "\\4\\wc.lst", false);
                break;
            }
            case 5: {//analysis fd

                sc = new Scanner(new File(path + "\\5\\fd.trace"));
                writer = new FileWriter(path + "\\5\\fd.lst", false);
                break;
            }
            case 6: {//analysis lg

                sc = new Scanner(new File(path + "\\6\\lg.trace"));
                writer = new FileWriter(path + "\\6\\lg.lst", false);
                break;
            }
            case 7: {//analysis sd

                sc = new Scanner(new File(path + "\\7\\sd.trace"));
                writer = new FileWriter(path + "\\7\\sd.lst", false);
                break;
            }
            case 8: {//analysis vs

                sc = new Scanner(new File(path + "\\8\\vs.trace"));
                writer = new FileWriter(path + "\\8\\vs.lst", false);
                break;
            }
            case 9: {//analysis tm

                sc = new Scanner(new File(path + "\\9\\tm.trace"));
                writer = new FileWriter(path + "\\9\\tm.lst", false);
                break;
            }
            case 10: {//analysis lr

                sc = new Scanner(new File(path + "\\10\\lr.trace"));
                writer = new FileWriter(path + "\\10\\lr.lst", false);
                break;
            }
        }

        String r;
        String[] r_a;
        String function;
        while (!sc.next().startsWith("Process_Number_ID")) {
            sc.next();
        }
        sc.nextLine();
        LinkedList<record> list = new LinkedList<>();
        while (sc.hasNextLine()) {
            String read = sc.nextLine();
            String[] read_s = read.split(" ");
            list.add(new record(read_s[0], read_s[1], read_s[2], read_s[3], read_s[4], read_s[5], read_s[6], read_s[7]));
        }

//        while (sc.hasNextLine()) {
//            r_a = read_record(sc);
//            //4: number of instructions
//            //6: symbol of function
//
//            event_trace.add(r_a[6]);
//        }
        return list;
    }

    public static void main(String[] arg) throws IOException {

        int policy = 1;//0:random, 1:LRU...

        for (int app = 4; app < 4; app++) {

            LinkedList<record> event_trace = clean_InTrace_results(app);

        }
    }

    private static class record {
        /*
        * Column Labels:
  LV         :: Level of Nesting       (Call Depth)
  CALLS      :: Calls to this Method   (Callers)
  CEE        :: Calls from this Method (Callees)
  PTT_INSTS  :: Per Thread Instructions
  PTT_CYCLES :: Per Thread Cycles
  DS         :: Dispatches Observed
  IN         :: Interrupts Observed
  NAME       :: Name of Method or Thread
        * */
        public int LV;
        public int CALLS;
        public int CEE;
        public int PTT_INSTS;
        public int PTT_CYCLES;
        public int DS;
        public int IN;
        public String NAME;

        record(String LV,
               String CALLS,
               String CEE,
               String PTT_INSTS,
               String PTT_CYCLES,
               String DS,
               String IN,
               String NAME) {
            this.LV = Integer.parseInt(LV);
            this.CALLS = Integer.parseInt(CALLS);
            this.CEE = Integer.parseInt(CEE);
            this.PTT_INSTS = Integer.parseInt(PTT_INSTS);
            this.PTT_CYCLES = Integer.parseInt(PTT_CYCLES);
            this.DS = Integer.parseInt(DS);
            this.IN = Integer.parseInt(IN);
            this.NAME = NAME;
        }
    }
}
