package utils;

import java.util.Random;

/**
 * store the static config for the app
 */
public class AppConfig {
    static Random random = new Random();
    public static int complexity;
    public static boolean isCyclic = true;
    public static int windowSize = 1024; // default window size is 1024

//    public static long randomDelay() {
//        int delay = random.nextInt(complexity);
////        int delay = complexity;
//        long sum = 1L;
//        for (int i = 1; i < delay; i++) {
//            sum *= i;
//        }
//        return sum;
//    }

//    public static byte[] randomDelay() {
//        try {
//            byte[] b = new byte[0];
//            for (int i = 1; i < 5; i++) {
                    // TODO: new a instance to avoid resource contention
//                MessageDigest digest = MessageDigest.getInstance("SHA-256");
//                b = digest.digest(("XXXXXXXXXXXXXXXXXXXX"+i).getBytes(StandardCharsets.UTF_8));
//            }
//            return b;
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    // TODO: we follow the OSDI'18 approach to do the computational complexity
    public static void randomDelay() {
        long start = System.nanoTime();
        while (System.nanoTime() - start < complexity) {}
    }
}
