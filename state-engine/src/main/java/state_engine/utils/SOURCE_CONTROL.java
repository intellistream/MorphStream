package state_engine.utils;
import state_engine.common.SpinLock;

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
    private int number_threads;
    private CyclicBarrier start_barrier;
    private CyclicBarrier end_barrier;
    private CyclicBarrier final_end_barrier;
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
        this.number_threads = number_threads;
        start_barrier = new CyclicBarrier(number_threads);
        end_barrier = new CyclicBarrier(number_threads);
        final_end_barrier = new CyclicBarrier(number_threads);
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
    public void Wait_Start(int thread_Id) {
//        this.wm.incrementAndGet();
//        //busy waiting
//        while (!this.wm.compareAndSet(this.number_threads, 0)) {
//            //not ready for this thread to proceed! Wait for other threads
//            if (Thread.currentThread().isInterrupted()) {
//                throw new InterruptedException();
//            }
//        }
//        Integer itr = iteration.get(thread_Id);
//
//        if (itr > min_iteration() + 1) {
//            Log.info(thread_Id + " is running too fast");
//        }
        try {
            start_barrier.await();
        } catch (Exception ex) {
//            e.printStackTrace();
        }
//        iteration.put(thread_Id, itr + 1);
//        assert barrier.getNumberWaiting() == 0;
    }
    public void Wait_End(int thread_Id) {
//        this.wm.incrementAndGet();
//        //busy waiting
//        while (!this.wm.compareAndSet(this.number_threads, 0)) {
//            //not ready for this thread to proceed! Wait for other threads
//            if (Thread.currentThread().isInterrupted()) {
//                throw new InterruptedException();
//            }
//        }
//        Integer itr = iteration.get(thread_Id);
//
//        if (itr > min_iteration() + 1) {
//            Log.info(thread_Id + " is running too fast");
//        }
        try {
            end_barrier.await();
        } catch (Exception ex) {
//            e.printStackTrace();
        }
//        iteration.put(thread_Id, itr + 1);
//        assert barrier.getNumberWaiting() == 0;
    }


    private int barriersCreatedForDependencyLevel = -1;
    private CyclicBarrier dLevelStartBarrier;
    private CyclicBarrier dLevelEndBarrier;

    public void createBarrierForDependencyLevel(int dLevel) {
        if(dLevel == barriersCreatedForDependencyLevel)
            return;
        barriersCreatedForDependencyLevel = dLevel;

        dLevelStartBarrier = new CyclicBarrier(number_threads);
        dLevelEndBarrier = new CyclicBarrier(number_threads);
    }

    public void Wait_Start_For_Evaluation(int thread_Id) {
        try {
            dLevelStartBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void Wait_End_For_Evaluation(int thread_Id) {
        try {
            dLevelEndBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void OneThreadFinished(int thread_Id) {
        number_threads-=1;
        System.out.println("Remaining thread: "+number_threads);
    }



    public void Final_END(int thread_Id) {
//        this.wm.incrementAndGet();
//        //busy waiting
//        while (!this.wm.compareAndSet(this.number_threads, 0)) {
//            //not ready for this thread to proceed! Wait for other threads
//            if (Thread.currentThread().isInterrupted()) {
//                throw new InterruptedException();
//            }
//        }
//        Integer itr = iteration.get(thread_Id);
//
//        if (itr > min_iteration() + 1) {
//            Log.info(thread_Id + " is running too fast");
//        }
        try {
            final_end_barrier.await();
        } catch (Exception ex) {
//            e.printStackTrace();
        }
//        iteration.put(thread_Id, itr + 1);
//        assert barrier.getNumberWaiting() == 0;
    }
}
