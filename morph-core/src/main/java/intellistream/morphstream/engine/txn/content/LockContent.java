package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.List;

public abstract class LockContent implements Content {
    @Override
    public boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
        System.exit(-1);
    }

    @Override
    public boolean RequestWriteAccess(long timestamp, List<DataBox> data) {
        throw new UnsupportedOperationException();
    }

    //not in use.
    @Override
    public void RequestAbort(long timestamp) {
        System.exit(-1);
    }

    @Override
    public long GetLWM() {
        throw new UnsupportedOperationException();
    }

    //	@Override
//	public LWMContentImpl.XLockQueue GetXLockQueue() {
//		return null;
//	}
//
    @Override
    public SchemaRecord ReadAccess(FunctionContext context, CommonMetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readValues(long ts, long previous_mark_ID, boolean clean) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
        System.exit(-1);
    }

    @Override
    public boolean AcquireCertifyLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void WriteAccess(long commit_timestamp, long mark_ID, boolean clean, SchemaRecord local_record_) {
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
        return false;
    }

    @Override
    public void LockPartitions() {
    }

    @Override
    public void UnlockPartitions() {
    }

    @Override
    public SchemaRecord ReadAccess(long snapshotId, boolean clean) {
        return null;
    }

    @Override
    public int[] getReadLVs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getWriteLVs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] getLVs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateReadLv(int lsn, int partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateWriteLv(int lsn, int partition) {
        throw new UnsupportedOperationException();
    }
}
