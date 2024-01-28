package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.List;

public interface Content {
    boolean TryReadLock();

    boolean TryWriteLock();

    void SetTimestamp(long timestamp);

    long GetTimestamp();

    void ReleaseReadLock();

    void ReleaseWriteLock();

    /**
     * new API for ordering guarantee
     */
    boolean TryWriteLock(OrderLock lock, FunctionContext txn_context) throws InterruptedException;

    boolean TryReadLock(OrderLock lock, FunctionContext txn_context) throws InterruptedException;

    boolean AcquireReadLock();

    boolean AcquireWriteLock();

    //TO.
    boolean RequestWriteAccess(long timestamp, List<DataBox> data);

    boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready);

    void RequestCommit(long timestamp, boolean[] is_ready);

    void RequestAbort(long timestamp);

    //LWM
    long GetLWM();

    //	LWMContentImpl.XLockQueue GetXLockQueue();
    SchemaRecord ReadAccess(FunctionContext context, CommonMetaTypes.AccessType accessType);

    //Used to checkpoint the schema record
    SchemaRecord ReadAccess(long snapshotId, boolean clean);

    SchemaRecord readPreValues(long ts);

    SchemaRecord readPreValues(long ts, long min_ts);

    List<SchemaRecord> readPreValuesRange(long ts, long range);

    SchemaRecord readValues(long ts, long previous_mark_ID, boolean clean);

    void clean_map();

    void updateValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record);

    void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record);

    boolean AcquireCertifyLock();

    SchemaRecord ReadAccess(long ts, long mark_ID, boolean clean, CommonMetaTypes.AccessType accessType);

    void WriteAccess(long commit_timestamp, long mark_ID, boolean clean, SchemaRecord local_record_);

    void ReleaseCertifyLock();

    void AddLWM(long ts);

    void DeleteLWM(long ts);

    boolean TryLockPartitions();

    void LockPartitions();

    void UnlockPartitions();

    int[] getWriteLVs();//Used by LVStreamContent

    int[] getReadLVs();//Used by LVStreamContent

    int[] getLVs();//Used by LVStreamContent

    void updateWriteLv(int lsn, int partition);

    void updateReadLv(int lsn, int partition);
}
