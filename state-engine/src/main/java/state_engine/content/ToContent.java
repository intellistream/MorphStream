package state_engine.content;
import state_engine.Meta.MetaTypes;
import state_engine.common.OrderLock;
import state_engine.storage.SchemaRecord;
import state_engine.transaction.impl.TxnContext;
public abstract class ToContent implements Content {
    @Override
    public boolean TryReadLock() {
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
    public boolean TryWriteLock() {
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
    public long GetLWM() {
        throw new UnsupportedOperationException();
    }
    //	@Override
//	public LWMContentImpl.XLockQueue GetXLockQueue() {
//		return null;
//	}
    @Override
    public SchemaRecord ReadAccess(TxnContext context, MetaTypes.AccessType accessType) {
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
        return false;
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
}
