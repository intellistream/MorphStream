package utils;

import java.util.Random;

/**
 * store the static config for the app
 */
public class AppConfig {
    static Random random = new Random();
    public static int complexity;
    public static boolean isCyclic = true;

    public static long randomDelay() {
        int delay = random.nextInt(complexity);
        long sum = 1L;
        for (int i = 1; i < delay; i++) {
            sum *= i;
        }
        return sum;
    }
}
