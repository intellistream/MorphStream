package utils;
import common.SpinLock;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
public class SOURCE_CONTROL {
    //    static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy
    static SpinLock counterLock = new SpinLock();
    private static SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    volatile boolean success = false;
    private volatile long counter = 0;
    private AtomicInteger wm = new AtomicInteger(0);// it is already volatiled.

    private int totalThreads;
    private CyclicBarrier startBarrier;
    private CyclicBarrier endBarrier;
    private CyclicBarrier finalEndBarrier;

    private HashMap<Integer, CyclicBarrier> dLevelBarriers;
    private CyclicBarrier dLevelEndBarrier;
    private int completedThreadsCount;
    private int dependencyLevelToProcess=-1;

    //    private long _combo_bid_size;
    private HashMap<Integer, Integer> iteration;
    public static SOURCE_CONTROL getInstance() {
        return ourInstance;
    }
    //return the starting point of counter.
//    public long GetAndUpdate() {
//        counterLock.lock();
//        long rt = counter;
//
//        counter += _combo_bid_size;//increment counter by combo_bid_size times...
//
//        counterLock.unlock();
//
//        return rt;
//    }
    public void config(int number_threads) {
        totalThreads = number_threads;
        startBarrier = new CyclicBarrier(number_threads);
        endBarrier = new CyclicBarrier(number_threads);
        finalEndBarrier = new CyclicBarrier(number_threads);

        dLevelBarriers = new HashMap<>();
        for(int threadCount = 1; threadCount<=totalThreads; threadCount++) {
            dLevelBarriers.put(threadCount, new CyclicBarrier(threadCount));
        }

        iteration = new HashMap<>();
        for (int i = 0; i < number_threads; i++) {
            iteration.put(i, 0);
        }
//        this._combo_bid_size = _combo_bid_size;
    }

    //return counter.
    public long Get() {
        return counter;
    }
    private int min_iteration() {
        return Collections.min(iteration.values());
    }
    public void preStateAccessBarrier(int threadId) {
        try {
            if(threadId==0){
                completedThreadsCount = 0;
                dependencyLevelToProcess=-1;
                updateThreadBarrierOnDLevel(0);
            }
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
            dLevelEndBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public synchronized void oneThreadCompleted() {
        completedThreadsCount++;
    }

    public synchronized void updateThreadBarrierOnDLevel(int dLevelToProcess) {
        if(completedThreadsCount==totalThreads || dependencyLevelToProcess == dLevelToProcess)
            return;
        dependencyLevelToProcess = dLevelToProcess;
        dLevelEndBarrier = dLevelBarriers.get(totalThreads-completedThreadsCount);
    }

}