package content;

import content.common.CommonMetaTypes;
import lock.OrderLock;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.context.TxnContext;

import java.util.List;

public abstract class SStoreContent implements Content {
    public final static String SStore_Content = "SStoreContent";

    @Override
    public boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean RequestWriteAccess(long timestamp, List<DataBox> data) {
        throw new UnsupportedOperationException();
    }

    //not in use.
    @Override
    public void RequestAbort(long timestamp) {
        throw new UnsupportedOperationException();
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
    public SchemaRecord ReadAccess(TxnContext context, CommonMetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaRecord readValues(long ts, long previous_mark_ID, boolean clean) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
        throw new UnsupportedOperationException();
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

    @Override
    public boolean TryReadLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean TryWriteLock() {
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
