package application.tools.cacheSim;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator_PI_generic {

    static String path = "C:\\Users\\szhang026\\Documents\\Profile-experiments\\compatibility-tracing\\log\\genlib";
    private static int start = 0;
    private static int cnt = 100;
    private static int app = 4;
    private static boolean spilit_method = true;
    private static int method = 0;//method 0: system method+user method. 1: user method only.

    private static LinkedList<record> clean_InTrace_results(int app) throws IOException {
        Scanner sc = null;
        switch (app) {
            case 3: {//analysis wc
                sc = new Scanner(new File(path + "\\null.trace"));
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
                }
                list.add(new record(Integer.parseInt(read_s[0]) % 8000, pre1, pre2, pre3));
                pre1 = read_s[1];
                pre2 = read_s[2];
                pre3 = read_s[3];
            }
        }
        list.removeFirst();
        return list;
    }

    public static void main(String[] arg) throws IOException {
        if (arg.length == 4) {
            start = Integer.parseInt(arg[0]);
            cnt = Integer.parseInt(arg[1]);
            app = Integer.parseInt(arg[2]);
            method = Integer.parseInt(arg[3]);
        }
        // for (int app = app_start; app < app_end; app++) {

        LinkedList<record> record_trace = clean_InTrace_results(app);
        LinkedList<String> event_trace = new LinkedList<>();
        LinkedList<String> flink_event_trace = new LinkedList<>();
        Map<String, Integer> Instruction_per_function = new HashMap<>();
        Map<String, Integer> Appears_per_function = new HashMap<>();

        Iterator<record> tr = record_trace.iterator();

        while (tr.hasNext()) {
            record i = tr.next();
            int scale = 1;
            int triped_inst = 1;
            String function;
            if (spilit_method) {
                triped_inst = i.PTT_INSTS < scale ? 1 : (int) (i.PTT_INSTS / (double) scale) * scale;
                function = i.NAME_M.concat(String.valueOf(triped_inst));
            } else {
                function = i.NAME_M;
            }
            int size_per_instruction = 1;//4 bytes

            if (i.EN_EX == 0) {
                event_trace.add(function);
                if (method == 1) {
                    if (function.contains("application")) {
                        flink_event_trace.add(function);
                        if (!Appears_per_function.containsKey(function)) {
                            Appears_per_function.put(function, 1);
                        } else {
                            Appears_per_function.put(function, Appears_per_function.get(function) + 1);
                        }
                    }
                } else {
                    flink_event_trace.add(function);
                    if (!Appears_per_function.containsKey(function)) {
                        Appears_per_function.put(function, 1);
                    } else {
                        Appears_per_function.put(function, Appears_per_function.get(function) + 1);
                    }
                }
            }
            if (spilit_method) {
                if (!Instruction_per_function.containsKey(function)) {
                    Instruction_per_function.put(function, triped_inst * size_per_instruction);
                }
            } else {
                //otherwise, we don't care instruction-per-function
            }
            tr.remove();
        }
        Appears_per_function = sortByValue(Appears_per_function);

        String[] arr = Arrays.copyOfRange(Appears_per_function.keySet().toArray(), start, cnt, String[].class);
        System.out.println("Start: " + start + "cnt:" + cnt);
        LinkedList<String> top_function = new LinkedList<>(Arrays.asList(arr));
        calculate_distribution(event_trace, top_function, Instruction_per_function, app, start, cnt);

    }

    private static void calculate_distribution(LinkedList<String> event_trace, final LinkedList<String> top_function
            , final Map<String, Integer> instruction_per_function, int app, int start, int cnt) {

        boolean[] first = new boolean[top_function.size()];
        PrintWriter[] bw = new PrintWriter[top_function.size()];
        // LinkedList<Integer> counter = new LinkedList();
        int[] counter = new int[top_function.size()];

        //initialize
        for (int i = 0; i < top_function.size(); i++) {
            counter[i] = 0;
            first[i] = true;
            File file;
            try {

                if (method == 1)
                    file = new File(path + "\\" + app + "\\app_function_gaps" + (i + start) + ".txt");
                else
                    file = new File(path + "\\" + app + "\\gaps" + (i + start) + ".txt");

                file.getParentFile().mkdirs();
                bw[i] = new PrintWriter(new FileWriter(file, false), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int total_size = event_trace.size();
        Iterator<String> tr = event_trace.iterator();
        while (tr.hasNext()) {
            String e = tr.next();
            for (int i = 0; i < counter.length; i++) {
                if (!first[i])
                    counter[i] += instruction_per_function.get(e);
            }

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
        System.out.println("Start clean");
        for (int i = 0; i < top_function.size(); i++) {
            bw[i].close();
        }
        System.out.println("Finished clean");
        System.exit(0);
    }

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