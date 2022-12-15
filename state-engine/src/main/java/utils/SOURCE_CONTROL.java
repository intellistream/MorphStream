package utils;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;

/**
 * One Stage One Control.
 */
public class SOURCE_CONTROL {

    private final long counter = 0;
    private int totalThreads;

    //TODO: BUG: As we can't live with compo design for ED, we need to reformat this part of code.
    private CyclicBarrier startBarrier;
    private CyclicBarrier endBarrier;
    private CyclicBarrier finalEndBarrier;
    private CyclicBarrier switchSchedulerBarrier;
    private CyclicBarrier exploreTPGBarrier;
    private Phaser dLevelEndBarrier;

    private HashMap<Integer, Integer> iteration;

    public void config(int number_threads) {
        totalThreads = number_threads;
        startBarrier = new CyclicBarrier(number_threads);
        endBarrier = new CyclicBarrier(number_threads);
        finalEndBarrier = new CyclicBarrier(number_threads);
        switchSchedulerBarrier = new CyclicBarrier(number_threads);
        exploreTPGBarrier = new CyclicBarrier(number_threads);
        dLevelEndBarrier = new Phaser(number_threads);
        iteration = new HashMap<>();
        for (int i = 0; i < number_threads; i++) {
            iteration.put(i, 0);
        }
    }

    //return counter.
    public long Get() {
        return counter;
    }

    public void preStateAccessBarrier(int threadId) {
        try {
            startBarrier.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void exploreTPGBarrier(int threadId) {
        try {

            exploreTPGBarrier.await();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void postStateAccessBarrier(int threadId) {
        try {
            endBarrier.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void finalBarrier(int threadId) {
        try {
            finalEndBarrier.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void checkFinalBarrier(int threadId) {
        try {
            finalEndBarrier.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void waitForOtherThreads(int threadId) {
        try {
            dLevelEndBarrier.arriveAndAwaitAdvance();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void waitForOtherThreadsAbort() {
        try {
            System.out.println("phase: " + dLevelEndBarrier.getPhase());
            dLevelEndBarrier.arriveAndAwaitAdvance();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void waitForSchedulerSwitch(int threadId) {
        try {
            switchSchedulerBarrier.await();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void oneThreadCompleted(int threadId) {
        dLevelEndBarrier.arriveAndDeregister();
    }

}