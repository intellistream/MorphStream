package lock;

import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionedOrderLock implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedOrderLock.class);
    private static final long serialVersionUID = 1347267778748318967L;
    private static final PartitionedOrderLock ourInstance = new PartitionedOrderLock();
    HashMap<Integer, LOCK> locks = new HashMap<>();
    boolean initilize = false;

    private PartitionedOrderLock() {
    }

    public static PartitionedOrderLock getInstance() {
        return ourInstance;
    }

    public LOCK get(int pid) {
        return locks.get(pid);
    }

    /**
     * Initilize locks.
     * This method shall be called only once.
     *
     * @param tthread
     */
    public void initilize(int tthread) {
        if (!initilize) {
            for (int i = 0; i < tthread; i++) {
                locks.put(i, new LOCK());
            }
            initilize = true;
        }
    }

    public class LOCK {
        public volatile AtomicDouble bid = new AtomicDouble();

        public boolean blocking_wait(final double bid, double _bid) {
//            if (!this.counter.compareAndSet(counter, counter))
//                if (enable_log) LOG.info("not ready for this batch to proceed:" + counter + " lock_ratio @" + this);
            while (!this.bid.compareAndSet(bid, bid)) {
                //not ready for this batch to proceed! Wait for previous batch to finish execution.
                if (Thread.currentThread().isInterrupted()) {
//				 throw new InterruptedException();
                    return false;
                }
            }
            return true;
        }

        public void advance() {
            bid.addAndGet(1);//allow next batch to proceed.
        }

        public void reset() {
            bid.set(0);
        }

        @Override
        public String toString() {
            return String.valueOf(bid.get());
        }
    }
}
