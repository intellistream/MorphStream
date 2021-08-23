package utils;

import lock.SpinLock;

import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

public class SOURCE_CONTROL {
    private static final SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    static SpinLock counterLock = new SpinLock();
    private final long counter = 0;
    private final AtomicInteger wm = new AtomicInteger(0);// it is already volatiled.
    volatile boolean success = false;
    private int totalThreads;
    private CyclicBarrier startBarrier;
    private CyclicBarrier endBarrier;
    private CyclicBarrier finalEndBarrier;

    private Phaser dLevelEndBarrier;
    private HashMap<Integer, Integer> iteration;

    public static SOURCE_CONTROL getInstance() {
        return ourInstance;
    }

    public void config(int number_threads) {
        totalThreads = number_threads;
        startBarrier = new CyclicBarrier(number_threads);
        endBarrier = new CyclicBarrier(number_threads);
        finalEndBarrier = new CyclicBarrier(number_threads);
        dLevelEndBarrier = new Phaser(number_threads);
        Phaser abortBarrier = new Phaser(number_threads);
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

    public void waitForOtherThreads() {
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

    public void oneThreadCompleted() {
        dLevelEndBarrier.arriveAndDeregister();
    }

}