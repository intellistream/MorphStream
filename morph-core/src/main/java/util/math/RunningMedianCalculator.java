package util.math;

import java.io.Serializable;
import java.util.PriorityQueue;

/**
 * Adopted from http://www.dsalgo.com/2013/02/RunningMedian.php.html
 * Author: Thilina
 * Date: 11/22/14
 */
public class RunningMedianCalculator implements Serializable {
    private static final long serialVersionUID = 16L;
    PriorityQueue<Double> upperQueue;
    PriorityQueue<Double> lowerQueue;

    public RunningMedianCalculator() {
        lowerQueue = new PriorityQueue<>(1000, (o1, o2) -> -o1.compareTo(o2));
        upperQueue = new PriorityQueue<>();
        upperQueue.add(Double.MAX_VALUE);
        lowerQueue.add(Double.MIN_VALUE);
    }

    public double getMedian(double num) {
        //adding the number to proper heap
        if (num >= upperQueue.peek())
            upperQueue.add(num);
        else
            lowerQueue.add(num);
        balance();
        //returning the median
        if (upperQueue.size() == lowerQueue.size())
            return (upperQueue.peek() + lowerQueue.peek()) / 2.0;
        else if (upperQueue.size() > lowerQueue.size())
            return upperQueue.peek();
        else
            return lowerQueue.peek();
    }

    private void balance() {
        //balancing the heaps
        if (upperQueue.size() - lowerQueue.size() == 2)
            lowerQueue.add(upperQueue.poll());
        else if (lowerQueue.size() - upperQueue.size() == 2)
            upperQueue.add(lowerQueue.poll());
    }

    public void remove(Double value) {
        if (upperQueue.contains(value)) {
            upperQueue.remove(value);
        } else {
            lowerQueue.remove(value);
        }
        balance();
    }
}