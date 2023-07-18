package util.tools.cacheSim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator_PI_generic_bak {
    static String path = "C:\\Users\\szhang026\\Documents\\Profile-experiments\\compatibility-tracing\\log\\genlib";
    private static int start = 0;
    private static int cnt = 100;
    private static int app = 4;

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

    //    private static Vector2D cacheHit(LinkedList<String> event_trace, Map<String, Integer> instruction_per_function, int policy) throws IOException {
//        final int cache_size = 32000;
//        int cache_used = 0;
//        int compulsory_miss = 0;
//        int access_miss = 0;
//        // LinkedList<Integer> cached = new LinkedList();
//        Map<String, Integer> cached = new HashMap<>();//<function name, age>
//        for (String i : event_trace) {
//            if (policy == 1) {
//                //update age
//                for (String key : cached.keySet()) {
//                    cached.put(key, cached.get(key) + 1);
//                }
//            }
//            assert cache_used == getsum(cached.keySet(), instruction_per_function);
//            if (!cached.containsKey(i)) {
//                if (cache_used + instruction_per_function.get(i) > cache_size) {
//
//                    switch (policy) {
//                        case 0: {
//                            while (cache_used + instruction_per_function.get(i) > cache_size) {
//                                assert (cache_used == getsum(cached.keySet(), instruction_per_function));
//                                Random random = new Random();
//                                List<String> keys = new ArrayList<String>(cached.keySet());
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
//                                try {
//                                    cache_used -= instruction_per_function.get(idx);
//                                } catch (Exception e) {
//                                    System.out.println("Err");
//                                }
//                            }
//                            break;
//                        }
//                    }
//
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
        PrintWriter writer = null;
        Scanner sc = null;
        switch (app) {
            case 3: {//analysis wc
                sc = new Scanner(new File(path + "\\test.trace"));
                break;
            }
            case 4: {//analysis wc
                sc = new Scanner(new File(path + "\\wc.trace"));
                break;
            }
            case 5: {//analysis fd
                sc = new Scanner(new File(path + "\\fd.trace"));
                break;
            }
            case 6: {//analysis lg
                sc = new Scanner(new File(path + "\\lg.trace"));
                break;
            }
            case 7: {//analysis sd
                sc = new Scanner(new File(path + "\\sd.trace"));
                break;
            }
            case 8: {//analysis vs
                sc = new Scanner(new File(path + "\\vs.trace"));
                break;
            }
            case 9: {//analysis tm
                sc = new Scanner(new File(path + "\\tm.trace"));
                break;
            }
            case 10: {//analysis lr
                sc = new Scanner(new File(path + "\\lr.trace"));
                break;
            }
        }
        File file = new File(path + "\\" + app + "_gaps.txt");
        file.getParentFile().mkdirs();
        writer = new PrintWriter(new FileWriter(file, false), true);
        String r;
        String[] r_a;
        String function;
        LinkedList<record> list = new LinkedList<>();
        String pre1 = "", pre2 = "", pre3 = "";
        while (sc.hasNextLine()) {
            String read = sc.nextLine().trim();
            String[] read_s = read.split(" ");
            if (read_s.length == 4) {
                int consequtive_exe = Integer.parseInt(read_s[0]) / 8000;
                for (int i = 0; i < consequtive_exe; i++) {
                    list.add(new record(8000, pre1, pre2, pre3));
                    pre1 = read_s[1];
                    pre2 = read_s[2].concat(String.valueOf(i));
                    pre3 = read_s[3];
                    writer.println(8000 * 4);
                }
                list.add(new record(Integer.parseInt(read_s[0]) % 8000, pre1, pre2, pre3));
                pre1 = read_s[1];
                pre2 = read_s[2];
                pre3 = read_s[3];
                writer.println(Integer.parseInt(read_s[0]) % 8000 * 4);
                writer.flush();
            }
        }
        writer.close();
        list.removeFirst();
        return list;
    }

    public static void main(String[] arg) throws IOException {
        if (arg.length > 3) {
            start = Integer.parseInt(arg[0]);
            cnt = Integer.parseInt(arg[1]);
            app = Integer.parseInt(arg[2]);
        }
        // for (int app = app_start; app < app_end; app++) {
        LinkedList<record> record_trace = clean_InTrace_results(app);
        LinkedList<String> event_trace = new LinkedList<>();
        Map<String, Integer> Instruction_per_function = new HashMap<>();
        Map<String, Integer> Appears_per_function = new HashMap<>();
        Iterator<record> tr = record_trace.iterator();
        while (tr.hasNext()) {
            record i = tr.next();
            int scale = 1;
            int triped_inst = i.PTT_INSTS < scale ? 1 : (int) (i.PTT_INSTS / (double) scale) * scale;
            int size_per_instruction = 4;//4 bytes
            String function = i.NAME_M.concat(String.valueOf(triped_inst));
            if (i.EN_EX == 0) {
                event_trace.add(function);
                if (!Appears_per_function.containsKey(function)) {
                    Appears_per_function.put(function, 1);
                } else {
                    Appears_per_function.put(function, Appears_per_function.get(function) + 1);
                }
            }
            if (!Instruction_per_function.containsKey(function)) {
                Instruction_per_function.put(function, triped_inst * size_per_instruction);
            }
            tr.remove();
        }
        Appears_per_function = sortByValue(Appears_per_function);
//            int sum = 0;
//            int total = 0;
//            for (int i : Appears_per_function.values()) {
//                if (cnt > 0) {
//                    sum += i;
//                    cnt--;
//                }
//                total += i;
//            }
//            System.out.print("Top:" + top + " functions occupy:");
//            System.out.printf("%.2f", (double) sum / total * 100);
//            System.out.println("% of total " + Appears_per_function.size() + " calling");
//            for(start = offset,cnt = top;) {
//                LinkedList<String> top_function = new LinkedList<String>();
//
//                for (String i : Appears_per_function.keySet().) {
//                    if (start > 0) {
//                        start--;
//                    } else if (cnt > 0) {
//                        top_function.add(i);
//                        cnt--;
//                    }
//                }
//                calculate_distribution(event_trace, top_function, Instruction_per_function, app);
//            }
//
//            int cnt = top;
//            int start = offset;
        // for (int start = top, cnt = top; cnt < final_top; start += top, cnt += top) {
//        String[] arr = Arrays.copyOfRange(Appears_per_function.keySet().toArray(), start, cnt, String[].class);
//        System.out.println("Start: " + start + "cnt:" + cnt);
//        LinkedList<String> top_function = new LinkedList<>(Arrays.asList(arr));
        //calculate_distribution(event_trace, top_function, Instruction_per_function, app, start, cnt);
//        calculate_distribution(event_trace,Instruction_per_function,app);
        //}
//            Vector2D result = cacheHit(event_trace, Instruction_per_function, policy);
//            System.out.println("compulsory_miss ratio:" + result.getX() / event_trace.size());
//            System.out.println("Access_miss ratio:" + result.getY() / event_trace.size());
        //System.out.println("Cache Miss:" + (result.getX() + result.getY() )/ event_trace.size());
        //}
    }

    private static void calculate_distribution(LinkedList<String> event_trace, final LinkedList<String> top_function
            , final Map<String, Integer> instruction_per_function, int app, int start, int cnt) {
        //final TreeMap<Integer, Integer>[] gaps = new TreeMap[top_function.size()];
        //LinkedList<Integer>[] list_gaps = new LinkedList[top_function.size()];
        boolean[] first = new boolean[top_function.size()];
        PrintWriter[] bw = new PrintWriter[top_function.size()];
        // LinkedList<Integer> counter = new LinkedList();
        int[] counter = new int[top_function.size()];
        //initialize
        for (int i = 0; i < top_function.size(); i++) {
            counter[i] = 0;
            first[i] = true;
            try {
                File file = new File(path + "\\" + app + "\\gaps" + (i + start) + ".txt");
                file.getParentFile().mkdirs();
                bw[i] = new PrintWriter(new FileWriter(file, false), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //gaps[i] = new TreeMap();
            //list_gaps[i] = new LinkedList<>();
        }
        //ExecutorService exec = Executors.newFixedThreadPool(1);
        int total_size = event_trace.size();
        Iterator<String> tr = event_trace.iterator();
        while (tr.hasNext()) {
            String e = tr.next();
            for (int i = 0; i < counter.length; i++) {
                if (!first[i])
                    counter[i] += instruction_per_function.get(e);
            }
//
//            counter.parallelStream()
//                    .forEach(el -> el += instruction_per_function.get(e));
//            for (int i = 0; i < counter.length; i++) {
//                int finalI = i;
//                int finalI1 = i;
//                exec.submit(new Runnable() {
//                    @Override
//                    public void run() {
//                        if (!first[finalI])
//                            counter[finalI] += instruction_per_function.get(e);
//
//                        if (e.equals(top_function.get(finalI1))) {
//                            if (first[finalI]) {//first time encounter
//                                first[finalI] = false;
//                                counter[finalI1] = 0;//clean pre-count
//                                //gaps[finalI1].put(0, 1);
//                                //list_gaps[finalI1].add(0);
//                            } else {
//                                counter[finalI1] -= instruction_per_function.get(e);//remove itself.
//                                //gaps[finalI1].put(counter[finalI1], (gaps[finalI1].get(counter[finalI1]) == null ? 0 : gaps[finalI1].get(counter[finalI1])) + 1);
//                                // list_gaps[finalI1].add(counter[finalI1]);
//                                bw[finalI1].println(counter[finalI1]);
//                                bw[finalI1].flush();
//                                counter[finalI1] = 0;
//                            }
//                        }
//                    }
//                });
//            }
            for (int i = 0; i < counter.length; i++) {//for each function.
                if (e.equals(top_function.get(i))) {
                    if (first[i]) {//first time encounter
                        first[i] = false;
                        counter[i] = 0;//clean pre-count
                    } else {
                        counter[i] -= instruction_per_function.get(e);//remove itself.
                        bw[i].println(counter[i]);
                        bw[i].flush();
                        counter[i] = 0;
                    }
                }
            }
            cnt++;
            if (cnt % 1000 == 0) {
                System.out.print("Processed: " + cnt + " over: " + total_size);
                System.out.printf(" that's %.2f", (double) cnt / total_size * 100);
                System.out.println("% processed");
            }
            tr.remove();
        }
        System.out.println("Finished process");
//
//        try {
//            int i = 0;
//            for (TreeMap<Integer, Integer> m : gaps) {
//                PrintWriter bw = new PrintWriter(new FileWriter(new File(path + "\\gaps" + (i++) + ".txt")));
//                for (Map.Entry<Integer, Integer> entry : m.entrySet()) {
//                    bw.println(entry.getKey() + " " + entry.getValue());
//                }
//            }
//
//            //           bw.write(list_gaps.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.println("Start clean");
        for (int i = 0; i < top_function.size(); i++) {
            bw[i].close();
        }
        System.out.println("Finished clean");
        System.exit(0);
    }

    //
//    private static void calculate_distribution(LinkedList<String> event_trace, Map<String, Integer> instruction_per_function, int app) {
//        int total_size = event_trace.size();
//        Iterator<String> tr = event_trace.iterator();
//        int counter=0;
//        while (tr.hasNext()) {
//            String e = tr.next();
//
//        }
//    }
    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list =
                new LinkedList<>(map.entrySet());
        list.sort((o1, o2) -> (o1.getValue()).compareTo(o2.getValue()));
        Collections.reverse(list);
        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static class record {
        /*
        * Column Labels:
            PTT_INSTS  :: Per Thread Instructions
            EN_EX      :: Method Enter or Method Exit
            NAME_M     :: Name of Method
            NAME_T     :: Name or Thread
        * */
        public int PTT_INSTS;
        public int EN_EX;//0 means enter, 1 means exit
        public String NAME_M;
        public String NAME_T;

        public record(int read_0, String read_1, String read_2, String read_3) {
            PTT_INSTS = read_0;
            if (read_1.equalsIgnoreCase("<")) {
                EN_EX = 1;
            } else {
                EN_EX = 0;
            }
            NAME_M = read_2;
            NAME_T = read_3;
        }
    }
}