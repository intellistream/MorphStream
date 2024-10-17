package intellistream.morphstream.transNFV.common;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class S2PLLockObject {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final PriorityQueue<LockRequest> lockQueue; // Sort both read and write request locks by timestamp
    private boolean hasExclusiveLock = false;
    private int sharedLockCount = 0;

    public S2PLLockObject(int numExecutors) {
        lockQueue = new PriorityQueue<>(numExecutors, Comparator.comparingLong(lr -> lr.timestamp));
    }

    public void acquireLock(long timestamp, boolean isWrite) throws InterruptedException {
        lock.lock();
        try {
            LockRequest request = new LockRequest(timestamp, isWrite);
            lockQueue.add(request);

            // Blocked
            while (!request.equals(lockQueue.peek())
//                        || lockQueue.size() < S2PLLockManagerOrdered.numExecutors
                    || (isWrite && (hasExclusiveLock || sharedLockCount > 0))
                    || (!isWrite && hasExclusiveLock)) {
                condition.await();  // Wait on the shared condition
            }

            // Acquired
            lockQueue.poll();
            if (isWrite) {
                hasExclusiveLock = true;
            } else {
                sharedLockCount++;
            }

        } finally {
            lock.unlock();
        }
    }

    public void releaseLock(long timestamp) {
        lock.lock();
        try {
            if (hasExclusiveLock) {
                hasExclusiveLock = false;
            } else if (sharedLockCount > 0) {
                sharedLockCount--;
            }

            // Signal the next lock request in the queue
            if (!lockQueue.isEmpty()) {
                condition.signalAll();  // Signal all waiting threads
            }
        } finally {
            lock.unlock();
        }
    }

}
