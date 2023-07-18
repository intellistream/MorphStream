package util.tools.cacheSim;

import java.util.Random;

/**
 * Created by I309939 on 7/29/2016.
 */
public class randomNumberGenerator {
    public static int generate(int min, int max) {
        final Random rn = new Random(System.nanoTime());
        int result = rn.nextInt(max - min + 1) + min;
        //System.out.println(result);
        return result;
    }
}
