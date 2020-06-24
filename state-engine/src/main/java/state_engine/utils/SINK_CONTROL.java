package state_engine.utils;

import state_engine.common.SpinLock;

import java.util.concurrent.locks.ReentrantLock;

import static application.CONTROL.sink_combo_bid_size;

public class SINK_CONTROL {

    static ReentrantLock counterLock = new ReentrantLock(true); // enable fairness policy
    private static SINK_CONTROL ourInstance = new SINK_CONTROL();
    public double throughput = 0;

    SpinLock lock = new SpinLock();
    private volatile int counter = 0;
    private int _combo_bid_size;

    public static SINK_CONTROL getInstance() {
        return ourInstance;
    }

    public void lock() {
        lock.lock();
    }

    public boolean try_lock() {
        return lock.Try_Lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void config() {
//        this._combo_bid_size = _combo_bid_size;//it must be one for LAL, LWM, and PAT.
    }


    //return the starting point of counter.
    public int GetAndUpdate() {
        counterLock.lock();
        int rt = counter;
        // Always good practice to enclose locks in a try-finally block
        try {
            counter += sink_combo_bid_size;//increment counter by combo_bid_size times...
        } finally {
            counterLock.unlock();
        }
        return rt;
    }


}
