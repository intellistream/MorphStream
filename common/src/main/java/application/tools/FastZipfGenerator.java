package application.tools;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class FastZipfGenerator {

    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map = new TreeMap<>();


    public FastZipfGenerator(int size, double skew, int offset) {


        computeMap(size, skew, offset);
    }

    private void computeMap(
            int size, double skew, int offset) {

        double div = 0;
        for (int i = 1; i <= size; i++) {
            div += (1 / Math.pow(i, skew));
        }

        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1 + offset);
        }

    }

    public int next() {
        double value = random.nextDouble();

        return map.ceilingEntry(value).getValue();
    }

    public void show_sample() {
        for (int i = 0; i < 100; i++) {
            System.out.println(this.next());
        }
    }


}
