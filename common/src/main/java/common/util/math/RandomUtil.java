package common.util.math;
import java.util.Random;
/**
 * @author maycon
 */
public class RandomUtil {
    private static Random rand = new Random();
    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see Random#nextInt(int)
     */
    public static int randInt(int min, int max) {
        // Usually this can be a field rather than a method variable
        //Random rand = new Random();
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }
    public static double randDouble(double min, double max) {
        //Random rand = new Random();
        double randomNum = min + (rand.nextDouble() * ((max - min) + 1));
        return randomNum;
    }
    public static int randomMinMax(int min, int max) {
        return min + (int) (rand.nextDouble() * (double) (max - min + 1));
    }
}
