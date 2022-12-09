package content;

import lock.OrderLock;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.context.TxnContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class TStreamContent implements Content {
    public final static String T_STREAMCONTENT = "T_STREAMCONTENT";
    public ConcurrentSkipListMap<Long, SchemaRecord> versions = new ConcurrentSkipListMap<>();//TODO: In fact... there can be at most only one write to the d_record concurrently. It is safe to just use sorted hashmap.
    public SchemaRecord record;

    //    private SpinLock spinlock_ = new SpinLock();
    @Override
    public boolean TryReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void SetTimestamp(double timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double GetTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ReleaseReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ReleaseWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock(OrderLock lock, TxnContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryReadLock(OrderLock lock, TxnContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean AcquireReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean AcquireWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean RequestWriteAccess(long timestamp, List<DataBox> data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestAbort(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double GetLWM() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param ts
     * @return
     */
    @Override
    public SchemaRecord readPreValues(long ts) {
//            spinlock_.lock_ratio();
        SchemaRecord record_at_ts = null;
        Map.Entry<Long, SchemaRecord> entry = versions.lowerEntry(ts);//always get the original (previous) version.
        if (entry != null) {
            record_at_ts = entry.getValue();
        } else
            record_at_ts = versions.get(ts);//not modified in last round
        if (record_at_ts == null || record_at_ts.getValues() == null)
            System.out.println("Read a null value??");
        return record_at_ts;
    }

    /**
     * @param ts
     * @return the version strictly matching ts.
     */
    @Override
    public SchemaRecord readCurrValues(long ts) {
        SchemaRecord record_at_ts = versions.get(ts);
        if (record_at_ts == null || record_at_ts.getValues() == null)
            System.out.println("TStreamContent: No record matching the current ts");
        return record_at_ts;
    }

    /**
     * @param ts
     * @return the latest version equal or less than ts
     */
    @Override
    public SchemaRecord readPastValues(long ts) {
        SchemaRecord record_at_ts = versions.floorEntry(ts).getValue();
        if (record_at_ts == null || record_at_ts.getValues() == null)
            System.out.println("TStreamContent: No record under the current ts");
        return record_at_ts;
    }

    /**
     * @param ts
     * @return the latest version equal or less than ts, return null if the latest key is less than min_ts
     */
    @Override
    public SchemaRecord readPastValues(long ts, long min_ts) {
        Map.Entry<Long, SchemaRecord> entry = versions.floorEntry(ts);
        if (entry.getKey() < min_ts) {
            return null;
        }
        SchemaRecord record_at_ts = entry.getValue();
        if (record_at_ts == null || record_at_ts.getValues() == null)
            System.out.println("TStreamContent: No record under the current ts");
        return record_at_ts;
    }

    @Override
    public SchemaRecord readPreRangeValues(long startTs, int range) {
        SchemaRecord record_in_range = null;
        return record_in_range;
    }

    /**
     * @param ts
     * @return null if the value does not reach the expected min_ts. otherwise, return the value.
     */
    @Override
    public SchemaRecord readPreValues(long ts, long min_ts) {
//            spinlock_.lock_ratio();
        SchemaRecord record_at_ts;
        Map.Entry<Long, SchemaRecord> entry = versions.lowerEntry(ts);//always get the original (previous) version.
        //not modified in last round
        record_at_ts = entry != null ? entry.getValue() : versions.get(ts);
        if (record_at_ts == null || record_at_ts.getValues() == null)
            System.out.println("Read a null value??");

        assert entry != null;
        if (entry.getKey() < min_ts) {
            return null;
        }
        return record_at_ts;
    }

    @Override
    public void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
        versions.put(ts, record);
    }

    public SchemaRecord readValues(long ts, long previous_mark_ID, boolean clean) {
        return record;
    }

    @Override
    public void clean_map() {
//        versions = (ConcurrentSkipListMap<Long, SchemaRecord>) versions.tailMap(versions.lastKey());
        versions.headMap(versions.lastKey(), false).clear();
        //update the record
        record.updateValues(versions.firstEntry().getValue().getValues());
    }

    @Override
    public void updateValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
        this.record = record;
    }

    @Override
    public boolean AcquireCertifyLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ReleaseCertifyLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void AddLWM(double ts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void DeleteLWM(long ts) {
        throw new UnsupportedOperationException();
    }

    //used in SStore
    @Override
    public boolean TryLockPartitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void LockPartitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void UnlockPartitions() {
        throw new UnsupportedOperationException();
    }
}
