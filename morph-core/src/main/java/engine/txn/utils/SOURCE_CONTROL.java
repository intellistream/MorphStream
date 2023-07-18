package engine.txn.utils;

import engine.txn.lock.SpinLock;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
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

    private CyclicBarrier switchSchedulerBarrier;

    private CyclicBarrier exploreTPGBarrier;

    private Phaser dLevelEndBarrier;

    private boolean isGroup;
    private int dalta;
    private CyclicBarrier[] exploreTPGBarrierByGroup;
    private Phaser[] dLevelEndBarrierByGroup;

    private HashMap<Integer, Integer> iteration;

    public static SOURCE_CONTROL getInstance() {
        return ourInstance;
    }

    public void config(int number_threads, int groupNum) {
        totalThreads = number_threads;
        startBarrier = new CyclicBarrier(number_threads);
        endBarrier = new CyclicBarrier(number_threads);
        finalEndBarrier = new CyclicBarrier(number_threads);
        switchSchedulerBarrier = new CyclicBarrier(number_threads);
        if (groupNum != 1){
            exploreTPGBarrierByGroup = new CyclicBarrier[groupNum];
            dLevelEndBarrierByGroup = new Phaser[groupNum];
            for (int i = 0; i < groupNum; i++) {
                exploreTPGBarrierByGroup[i] = new CyclicBarrier(number_threads / groupNum);
                dLevelEndBarrierByGroup[i] = new Phaser(number_threads / groupNum);
            }
            dalta = totalThreads / groupNum;
            isGroup = true;
        } else {
            exploreTPGBarrier = new CyclicBarrier(number_threads / groupNum);
            dLevelEndBarrier = new Phaser(number_threads / groupNum);
        }
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
            if (isGroup) {
                exploreTPGBarrierByGroup[threadId / dalta].await();
            } else {
                exploreTPGBarrier.await();
            }
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

    public void waitForOtherThreads(int threadId) {
        try {
            if (isGroup) {
                dLevelEndBarrierByGroup[threadId / dalta].arriveAndAwaitAdvance();
            } else {
                dLevelEndBarrier.arriveAndAwaitAdvance();
            }
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
     public void waitForSchedulerSwitch(int threadId){
        try{
            switchSchedulerBarrier.await();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
     }

    public void oneThreadCompleted(int threadId) {
        if (isGroup) {
            dLevelEndBarrierByGroup[threadId / dalta].arriveAndDeregister();
        } else {
            dLevelEndBarrier.arriveAndDeregister();
        }
    }

}