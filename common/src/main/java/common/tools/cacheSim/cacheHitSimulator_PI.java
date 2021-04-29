package common.tools.cacheSim;
import com.vividsolutions.jts.math.Vector2D;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator_PI {
    static String path = "/Users/shuhaozhang/compatibility-app/log";
    private static double nextTime(double rateParameter) {
        return -Math.log(1.0 - Math.random()) / rateParameter;
    }
    private static int getsum(Collection<String> l, Map<String, Integer> instruction_per_function) {
        int sum = 0;
        for (String i : l) {
            sum += instruction_per_function.get(i);
        }
        return sum;
    }
    private static Vector2D cacheHit(LinkedList<String> event_trace, Map<String, Integer> instruction_per_function, int policy) {
        final int cache_size = 32000;
        int cache_used = 0;
        int compulsory_miss = 0;
        int access_miss = 0;
        // LinkedList<Integer> cached = new LinkedList();
        Map<String, Integer> cached = new HashMap<>();//<function name, age>
        for (String i : event_trace) {
            if (policy == 1) {
                //update age
                for (String key : cached.keySet()) {
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
                                List<String> keys = new ArrayList(cached.keySet());
                                String randomKey = null;
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
                                String idx = null;
                                for (String key : cached.keySet()) {
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
    private static LinkedList<String> clean_InTrace_results(int app) throws IOException {
        LinkedList<String> event_trace = new LinkedList<>();
        FileWriter writer = null;
        Scanner sc = null;
        switch (app) {
            case 4: {//analysis wc
                sc = new Scanner(new File(path + "/4/wc.trace"));
                writer = new FileWriter(path + "/4/wc.lst", false);
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
        while (sc.hasNext()) {
            r = sc.next();
            if (r.contains("{")) {
                function = r.split("\\{")[0];
                r_a = function.split("]");
                function = r_a[r_a.length - 1];
                function = function.substring(1, function.length() - 2);
                function = function.replaceAll(":", "::");
                function = function.replaceAll("\\.", "::");
                event_trace.add(function);
                writer.write(function);
                writer.write("\n");
            }
        }
        writer.flush();
        writer.close();
        return event_trace;
    }
    public static void main(String[] arg) throws IOException {
        int policy = 1;//0:random, 1:LRU...
        for (int app = 4; app < 11; app++) {
            LinkedList<String> event_trace = clean_InTrace_results(app);
            //System.out.println(event_trace);
//            Map<String, Integer> instruction_per_function = new HashMap<>();
//
//            for (int i = 0; i < event_trace.size(); i++) {
//
//                if(instruction_per_function.get(event_trace.get(i))==null)
//                    instruction_per_function.put(event_trace.get(i), CaculateInst.calculate(path + "\\" + app + "\\" + event_trace.get(i).replaceAll("::", "__") + ".csv"));
//            }
//
//            Vector2D result = cacheHit(event_trace,     instruction_per_function, policy);
//            System.out.println("compulsory_miss ratio:" + result.getX() / event_trace.size());
//            System.out.println("Access_miss ratio:" + result.getY() / event_trace.size());
//            System.out.println("Cache Miss:" + result.getX() + result.getY() / event_trace.size());
        }
    }
}
