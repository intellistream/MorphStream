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

    public synchronized void createBarrierForDependencyLevel(int dLevel) {
        if(dLevel == barriersCreatedForDependencyLevel || number_threads==0)
            return;

        dLevelStartBarrier = new CyclicBarrier(number_threads);
        dLevelEndBarrier = new CyclicBarrier(number_threads);
        barriersCreatedForDependencyLevel = dLevel;
    }


    public void Wait_Start_For_Evaluation() {
        try {
            dLevelStartBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void Wait_End_For_Evaluation() {
        try {
            dLevelEndBarrier.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public synchronized void OneThreadFinished() {
        number_threads-=1;
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

//******* STATS BEGIN *******
//        Time spent in pre transaction                                             : 26.452%
//        Time spent in transaction processing                                      : 72.066%
//        Other time (read input, dump results to a file)                           : 1.483%
//        ******* PRE_TXN BREAKDOWN *******
//        Time spent creating Operation Chains                                      : 8.849%
//        Time spent recording data dependencies                                    : 12.168%
//        Time spent of recording data dependencies for out of transaction checking : 3.002%
//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 71.008%
//        Time spent on state accessing                                             : 1.056%
//        Time spent calculating levels                                             : 0.396%
//        Time spent on iterative processing                                        : 6.631%
//        Threads wait time and other overhead                                      : 63.981%
//
//
//        **************************************
//        ******* STATS BEGIN IN SECONDS *******
//        Total time                                                                : 266.070 seconds
//        Time spent in pre transaction                                             : 70.380 seconds
//        Time spent in transaction processing                                      : 191.745 seconds
//        Other time (read input, dump results to a file)                           : 3.945 seconds
//        ******* PRE_TXN BREAKDOWN *******
//        Time spent creating Operation Chains                                      : 23.544 seconds
//        Time spent recording data dependencies                                    : 32.374 seconds
//        Time spent of recording data dependencies for out of transaction checking : 7.988 seconds
//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 188.932 seconds
//        Time spent on state accessing                                             : 2.809 seconds
//        Time spent calculating levels                                             : 1.053 seconds
//        Time spent on iterative processing                                        : 17.643 seconds
//        Threads wait time and other overhead                                      : 170.236 seconds
//        ******* STATS ENDS *******

//        ******* STATS BEGIN *******
//        Time spent in pre transaction                                             : 28.720%
//        Time spent in transaction processing                                      : 68.093%
//        Other time (read input, dump results to a file)                           : 3.187%
//        ******* PRE_TXN BREAKDOWN *******
//        Time spent creating Operation Chains                                      : 10.418%
//        Time spent recording data dependencies                                    : 12.778%
//        Time spent of recording data dependencies for out of transaction checking : 2.493%
//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 67.003%
//        Time spent on state accessing                                             : 1.069%
//        Time spent calculating levels                                             : 1.518%
//        Time spent on iterative processing                                        : 41.239%
//        Threads wait time and other overhead                                      : 24.245%
//
//
//        **************************************
//        ******* STATS BEGIN IN SECONDS *******
//        Total time                                                                : 259.395 seconds
//        Time spent in pre transaction                                             : 74.499 seconds
//        Time spent in transaction processing                                      : 176.629 seconds
//        Other time (read input, dump results to a file)                           : 8.267 seconds
//        ******* PRE_TXN BREAKDOWN *******
//        Time spent creating Operation Chains                                      : 27.023 seconds
//        Time spent recording data dependencies                                    : 33.145 seconds
//        Time spent of recording data dependencies for out of transaction checking : 6.468 seconds
//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 173.802 seconds
//        Time spent on state accessing                                             : 2.772 seconds
//        Time spent calculating levels                                             : 3.939 seconds
//        Time spent on iterative processing                                        : 106.973 seconds
//        Threads wait time and other overhead                                      : 62.891 seconds
//        ******* STATS ENDS *******



//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 188.932 seconds
//        Time spent on state accessing                                             : 2.809 seconds
//        Time spent calculating levels                                             : 1.053 seconds
//        Time spent on iterative processing                                        : 17.643 seconds
//        Threads wait time and other overhead                                      : 170.236 seconds


//        ******* TRANSACTION PROCESSING BREAKDOWN *******
//        Time spent processing transactions                                        : 173.802 seconds
//        Time spent on state accessing                                             : 2.772 seconds
//        Time spent calculating levels                                             : 3.939 seconds
//        Time spent on iterative processing                                        : 106.973 seconds
//        Threads wait time and other overhead                                      : 62.891 seconds