package lock;

import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import static common.CONTROL.enable_log;

/**
 * Order lock_ratio should be globally shared.
 */
public class OrderLock implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(OrderLock.class);
    private static final long serialVersionUID = 1347267778748318967L;
    private static final OrderLock ourInstance = new OrderLock();
    //	SpinLock spinlock_ = new SpinLock();
//	volatile int fid = 0;
    AtomicDouble counter = new AtomicDouble(0);// it is already volatiled.
    SpinLock check_lock = new SpinLock();
    boolean wasSignalled = false;//to fight with missing signals.
    //	private transient HashMap<Integer, HashMap<Integer, Boolean>> executors_ready;//<FID, ExecutorID, true/false>
    private int end_fid;

    private OrderLock() {
    }

    public static OrderLock getInstance() {
        return ourInstance;
    }

    //	public int getFID() {
//		return fid;
//	}
    public double getBID() {
        return counter.get();
    }

    //	public synchronized void advanceFID() {
//		fid++;
//	}
//	public synchronized void try_fill_gap() {
//		counter.getAndIncrement();
////		fid = 0;
//	}
    public void setBID(long bid) {
        this.counter.set(bid);
    }

    protected void fill_gap(LinkedList<Long> gap) {
//		while (!gap.isEmpty()) {
//			try_fill_gap(gap.);
//			gap.getAndDecrement();
//		}
        for (int i = 0; i < gap.size(); i++) {
            Long g = gap.get(i);
            if (!try_fill_gap(g)) {
                return;
            }
        }
    }

    /**
     * fill the gap.
     *
     * @param g the gap immediately follows previous item.
     */
    public boolean try_fill_gap(Long g) {
        if (getBID() == g) {
            counter.addAndGet(1);//allow next batch to proceed.
            return true;
        }
        return false;
    }

    public boolean blocking_wait(final double bid) throws InterruptedException {

        /* busy waiting.
        while (!this.counter.compareAndSet(counter, counter)) {
            //not ready for this batch to proceed! Wait for previous batch to finish execution.
            if (Thread.currentThread().isInterrupted()) {
//				 throw new InterruptedException();
                return false;
            }
//			fill_gap(gap);
        }
        */


/*
        while (!this.counter.compareAndSet(counter, counter)) {
            if (enable_log)
                if (enable_log) LOG.trace("BLOCK WAITING FOR " + counter + " CURRENT COUNTER:" + this.counter + " Thread:" + Thread.currentThread().getName());
            synchronized (this.counter) {//this overhead is too high.
                if(!wasSignalled) {
                    this.counter.sync_ratio(1);
                }
            }
        }
        //clear signal and continue running.
        wasSignalled = false;
*/
        //busy waiting with sleep.
        while (!this.counter.compareAndSet(bid, bid)) {
            //not ready for this batch to proceed! Wait for previous batch to finish execution.
//            Thread.sleep(1);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
//                return false;
            }
        }
        return true;
    }

    public void advance() {
//		try_fill_gap();
//		try {
//			Thread.sleep(10);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}

/*
        long value = counter.incrementAndGet();//allow next batch to proceed.
        if (enable_log)
            if (enable_log) LOG.trace("ADVANCE BID to:" + value + " Thread:" + Thread.currentThread().getName());
        synchronized (this.counter) {
            wasSignalled = true;
            this.counter.notifyAll();
        }
*/
        double value = counter.addAndGet(1);//allow next batch to proceed.
        if (enable_log)
            if (enable_log) LOG.info("ADVANCE BID to:" + value + " Thread:" + Thread.currentThread().getName());
//		//if (enable_log) LOG.DEBUG(Thread.currentThread().getName() + " advance counter to: " + counter+ " @ "+ DateTime.now());
//		if (joinedOperators(txn_context)) {
////			advanceFID();//allow next operator to proceed.
//
//			if (txn_context.getFID() == end_fid) {
//
//			}
//			executors_ready_rest(txn_context);
//		}
    }
//	public void initial(HashMap<Integer, HashMap<Integer, Boolean>> map) {
//		executors_ready = map;
//	}
//
//	public void set_executor_ready(int fid, int task_id) {
//		executors_ready.get(fid).put(task_id, true);
//	}
//	/**
//	 * have received all tuples from source.
//	 *
//	 * @return
//	 */
//	private boolean all_executors_ready(int fid) {
//		return !(executors_ready.get(fid).containsValue(false));
//	}
//	public void setEnd_fid(int end_fid) {
//		this.end_fid = end_fid;
//	}
//	/**
//	 * If the fid corresponding executors all finished their execution.
//	 *
//	 * @param txnContext
//	 * @return
//	 */
//	private synchronized boolean joinedOperators(TxnContext txnContext) {
//		set_executor_ready(txnContext.getFID(), txnContext.getTaskID());
//		return all_executors_ready(txnContext.getFID());
//	}
//
//	private void executors_ready_rest(TxnContext txnContext) {
//		final HashMap<Integer, Boolean> map = executors_ready.get(txnContext.getFID());
//		for (int task_id : map.keySet()) {
//			map.put(task_id, false);
//		}
//	}
}
