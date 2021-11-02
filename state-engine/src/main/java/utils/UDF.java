package utils;

import java.util.Random;

public class UDF {
    static Random random = new Random();

    public static long randomDelay() {
        int delay = random.nextInt(1000000);
        long sum = 1L;
        for (int i = 1; i < delay; i++) {
            sum *= i;
        }
        return sum;
    }
}
