package intellistream.morphstream.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

public class zipf {
    public static void main(String[] args) throws IOException {
        int size = 99171;
        int sentenceLength = 10;
        Scanner sc = new Scanner(new File("C://Users//szhang026//Documents//Profile-experiments//TestingData//data//words"));
        HashMap hm = new HashMap();
        int m = 0;
        while (sc.hasNext()) {
            hm.put(m++, sc.next());
        }
        int n = 10000000;//10M words
        double skew = 0.0;
        //ZipfGenerator z0 = new ZipfGenerator(size, skew);
        FastZipfGenerator z1 = new FastZipfGenerator(size, skew, 0);
        //PrintWriter writer = new PrintWriter("C://Users//szhang026//Documents//Profile-experiments//TestingData//data//Skew0.dat", "UTF-8");
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File("C://Users//szhang026//Documents//Profile-experiments//TestingData//data//Skew0.dat"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 1; i <= n; i++) {
            int k = z1.next();
            //writer.print(k+" ");//for test
            if (i % sentenceLength != 0)
                bw.write(hm.get(k) + ",");
            if (i % sentenceLength == 0)
                bw.write(hm.get(k) + "\n");
            //counts.put(k, counts.get(k)+1);
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, Integer> computeCounts(
            ZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }

    private static Map<Integer, Integer> computeCounts(
            FastZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }
}

