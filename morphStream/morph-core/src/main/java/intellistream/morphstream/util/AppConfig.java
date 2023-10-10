package intellistream.morphstream.util;

import java.util.Random;

/**
 * store the static config for the app
 */
public class AppConfig {
    public static int complexity;
    public static boolean isCyclic = true;
    public static int windowSize = 1024; // default window size is 1024
    static Random random = new Random();

    // TODO: we follow the OSDI'18 approach to do the computational complexity
    public static void randomDelay() {
        long start = System.nanoTime();
        while (System.nanoTime() - start < complexity) {
        }
    }
}
