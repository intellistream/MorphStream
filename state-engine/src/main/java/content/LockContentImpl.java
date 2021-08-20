package content;

import content.common.CommonMetaTypes;
import lock.OrderLock;
import lock.RWLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import transaction.context.TxnContext;

import java.util.concurrent.atomic.AtomicLong;

/**
 * #elif defined(LOCK) || defined(OCC) || defined(SILO) || defined(ST)
 * LockContentImpl content_;
 */
public class LockContentImpl extends LockContent {
    public final static String LOCK_CONTENT = "LOCK_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(LockContentImpl.class);
    AtomicLong timestamp_ = new AtomicLong(0);
    RWLock lock_ = new RWLock();

    //used by non-blocking lock_ratio.
    @Override
    public boolean TryWriteLock(OrderLock orderLock, TxnContext txn_context) throws InterruptedException {
        return orderLock.blocking_wait(txn_context.getBID()) &&
                lock_.TryWriteLock();
    }

    @Override
    public boolean TryReadLock(OrderLock orderLock, TxnContext txn_context) throws InterruptedException {
        return orderLock.blocking_wait(txn_context.getBID()) &&
                lock_.TryReadLock();
    }

    @Override
    public boolean AcquireReadLock() {
        lock_.AcquireReadLock();
        return true;
    }

    @Override
    public boolean AcquireWriteLock() {
        lock_.AcquireWriteLock();
        return true;
    }

    @Override
    public SchemaRecord ReadAccess(long ts, long mark_ID, boolean clean, CommonMetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readPreValues(long ts) {
        return null;
    }

    @Override
    public void clean_map(long mark_ID) {
    }

    @Override
    public void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
    }

    @Override
    public boolean TryReadLock() {
        return lock_.TryReadLock();
    }

    @Override
    public boolean TryWriteLock() {
        return lock_.TryWriteLock();
    }

    @Override
    public void SetTimestamp(long timestamp) {
        timestamp_.set(timestamp);
    }

    @Override
    public long GetTimestamp() {
        return timestamp_.get();
    }

    @Override
    public void ReleaseReadLock() {
        lock_.ReleaseReadLock();
    }

    @Override
    public void ReleaseWriteLock() {
        lock_.ReleaseWriteLock();
    }
}
