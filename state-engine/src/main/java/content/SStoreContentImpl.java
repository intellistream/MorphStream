package content;

import com.google.common.util.concurrent.AtomicDouble;
import content.common.CommonMetaTypes;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;

public class SStoreContentImpl extends SStoreContent {
    public final static String SSTORE_CONTENT = "SSTORE_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(SStoreContentImpl.class);
    public final int pid;
    final SpinLock spinlock_;//Each partition has a spin lock.
    AtomicDouble timestamp_ = new AtomicDouble(0);

    public SStoreContentImpl(SpinLock[] spinlock_, int pid) {
        this.pid = pid;
        this.spinlock_ = spinlock_[pid];
    }

    @Override
    public void SetTimestamp(double timestamp) {
        timestamp_.set(timestamp);
    }

    @Override
    public double GetTimestamp() {
        return timestamp_.get();
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
    public SchemaRecord readCurrValues(long ts) {
        return null;
    }

    @Override
    public SchemaRecord readPastValues(long ts) {
        return null;
    }

    @Override
    public SchemaRecord readPastValues(long ts, long min_ts) {
        return null;
    }

    @Override
    public SchemaRecord readPreValues(long ts, long min_ts) {
        return null;
    }

    @Override
    public SchemaRecord readPreRangeValues(long startTs, int range) {
        return null;
    }

    @Override
    public void clean_map() {
    }

    @Override
    public void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
    }

    public boolean TryLockPartitions() {
        return spinlock_.Try_Lock();
    }

    public void LockPartitions() {
        spinlock_.lock();
    }

    public void UnlockPartitions() {
        spinlock_.unlock();
    }
}
