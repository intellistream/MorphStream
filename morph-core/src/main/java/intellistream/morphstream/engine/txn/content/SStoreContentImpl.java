package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SStoreContentImpl extends SStoreContent {
    public final static String SSTORE_CONTENT = "SSTORE_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(SStoreContentImpl.class);
    public final int pid;
    final SpinLock spinlock_;//Each partition has a spin lock.
    AtomicLong timestamp_ = new AtomicLong(0);

    public SStoreContentImpl(SpinLock[] spinlock_, int pid) {
        this.pid = pid;
        this.spinlock_ = spinlock_[pid];
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
    public SchemaRecord ReadAccess(long ts, long mark_ID, boolean clean, CommonMetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readPreValues(long ts) {
        return null;
    }

    @Override
    public SchemaRecord readPreValues(long ts, long min_ts) {
        return null;
    }

    @Override
    public List<SchemaRecord> readPreValuesRange(long ts, long range) {
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
