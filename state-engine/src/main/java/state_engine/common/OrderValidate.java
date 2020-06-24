package state_engine.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * used in Occ, but its performance is very bad.
 */
public class OrderValidate implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(OrderValidate.class);
    private static final long serialVersionUID = 905380243120316314L;
//	private static OrderLock ourInstance = new OrderLock();

//	SpinLock spinlock_ = new SpinLock();

    //	public static OrderLock getHolder() {
//		return ourInstance;
//	}
//	volatile int fid = 0;
    final AtomicLong bid = new AtomicLong();
    //	private transient HashMap<Integer, HashMap<Integer, Boolean>> executors_ready;//<FID, ExecutorID, true/false>
    private int end_fid;

    public OrderValidate() {

    }

//	public int getFID() {
//		return fid;
//	}

    public long getBID() {
        return bid.get();
    }

//	public synchronized void advanceFID() {
//		fid++;
//	}

//	public synchronized void try_fill_gap() {
//		counter.getAndIncrement();
////		fid = 0;
//	}

    public void setBID(long bid) {
        this.bid.set(bid);
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
            bid.incrementAndGet();//allow next batch to proceed.
            //LOG.DEBUG(Thread.currentThread().getName() + " advance counter to: " + counter + " @ " + DateTime.now());
            return true;
        }
        return false;
    }

    public boolean validate(final long bid) {

        if (!this.bid.compareAndSet(bid, bid)) {
            //not ready for this batch to proceed! Wait for previous batch to finish execution.
//			fill_gap(gap);
            return this.bid.compareAndSet(bid, bid);
        }
        return true;
    }

    public void advance() {

//		try_fill_gap();
        bid.incrementAndGet();//allow next batch to proceed.
        //LOG.DEBUG(Thread.currentThread().getName() + " advance counter to: " + counter + " @ " + DateTime.now());
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
