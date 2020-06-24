package application.util.math;

/**
 * A helper class to track the average per each house and each plug.
 * Author: Thilina
 * Date: 10/31/14
 */
public class AverageTracker {
    private int count;
    private double total;

    public void track(double value) {
        total += value;
        count++;
    }

    public double retrieve() {
        return total / count;
    }

    public void reset() {
        count = 0;
        total = 0;
    }

}