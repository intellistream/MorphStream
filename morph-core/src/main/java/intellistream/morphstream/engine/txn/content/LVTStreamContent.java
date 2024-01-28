package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/*
 * This class is used to store the content of a table for a specific version, which supports to implement the LSN Vector protocol.
 * Tuple.readLV() and Tuple.writeLV() are used to record the LSN Vector for read and write operation.
 * More detail can be found in the paper "Taurus: Lightweight Parallel Logging for In_Memory Database Management Systems".
 * Project Link: https://github.com/yuxiamit/DBx1000_logging.
 */
public abstract class LVTStreamContent implements Content {
    public final static String LVTSTREAM_CONTENT = "LVTSTREAM_CONTENT";
    public ConcurrentSkipListMap<Long, SchemaRecord> versions = new ConcurrentSkipListMap<>();//TODO: In fact... there can be at most only one write to the d_record concurrently. It is safe to just use sorted hashmap.
    public int[] readLV;
    public int[] writeLV;
    public SchemaRecord record;

    public int[] elemWiseMax() {
        int[] max = new int[readLV.length];
        for (int i = 0; i < readLV.length; i++) {
            max[i] = Math.max(readLV[i], writeLV[i]);
        }
        return max;
    }

    @Override
    public int[] getReadLVs() {
        return this.readLV;
    }

    @Override
    public int[] getWriteLVs() {
        return this.writeLV;
    }

    @Override
    public int[] getLVs() {
        return elemWiseMax();
    }

    public void setLVs(int tthread) {
        readLV = new int[tthread];
        writeLV = new int[tthread];
        for (int i = 0; i < tthread; i++) {
            readLV[i] = 0;
            writeLV[i] = 0;
        }
    }

    @Override
    public void updateReadLv(int lsn, int partition) {
        this.readLV[partition] = lsn;
    }

    @Override
    public void updateWriteLv(int lsn, int partition) {
        this.writeLV[partition] = lsn;
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

    @Override
    public List<SchemaRecord> readPreValuesRange(long ts, long range) {

        long start = ts - range < 0 ? 0 : ts - range;

        ConcurrentNavigableMap<Long, SchemaRecord> schemaRange = versions.tailMap(start);

        //not modified in last round
//        if (schemaRange.size() == 0)
//            System.out.println("Empty window");
//        else
//            System.out.println(schemaRange.size());

//        assert schemaRange.size() != 0;

        return new ArrayList<>(schemaRange.values());
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

    public SchemaRecord readValues(long snapshotId, boolean clean) {
        SchemaRecord schemaRecord = versions.lowerEntry(snapshotId).getValue();
        if (clean) {
            //TODO: clean old version
        }
        return schemaRecord;
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
    public boolean TryReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void SetTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long GetTimestamp() {
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
    public boolean TryWriteLock(OrderLock lock, FunctionContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryReadLock(OrderLock lock, FunctionContext txn_context) {
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
    public long GetLWM() {
        throw new UnsupportedOperationException();
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
    public void AddLWM(long ts) {
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
