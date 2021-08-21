package content;

import lock.OrderLock;
import lock.RWLock;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.context.TxnContext;

import java.util.List;
import java.util.TreeMap;

public abstract class LWMContent implements Content {
    public final static String LWM_CONTENT = "LWM_CONTENT";
    public TreeMap<Long, SchemaRecord> versions = new TreeMap<>();//In fact... there can be at most only one write to the d_record concurrently. It is safe to just use sorted hashmap.
    //	XLockQueue xLockQueue = new XLockQueue();
//	XLockQueue sLockQueue = new XLockQueue();
    RWLock rw_lock_ = new RWLock();

    //	class XLockQueue {
//		List<XLock> locks;
//		long lwm;
//
//
//		public void AddLock(XLock lock_ratio) {
//			locks.addOperation(lock_ratio);
//			AddLWM();
//		}
//
//		public void ReleaseLock(TxnContext txn) {
//			remove(locks, txn);
//		}
//
//		//﻿locks.Remove(all locks held by Ti)
//		private void remove(List<XLock> locks, TxnContext txn) {
//
//		}
//
//		private void AddLWM() {
//			long min = locks.get(0).ts;
//			for (XLock l : locks) {
//				min = min < l.ts ? min : l.ts;
//			}
//			lwm = min;
//		}
//	}
    @Override
    public boolean TryReadLock() {
        return rw_lock_.TryReadLock();
    }

    @Override
    public boolean TryWriteLock() {
        return rw_lock_.TryWriteLock();
    }

    @Override
    public void SetTimestamp(long timestamp) {
    }

    @Override
    public long GetTimestamp() {
        return 0;
    }

    @Override
    public boolean TryWriteLock(OrderLock lock, TxnContext txn_context) {
        return false;
    }

    @Override
    public boolean TryReadLock(OrderLock lock, TxnContext txn_context) {
        return false;
    }

    @Override
    public boolean RequestWriteAccess(long timestamp, List<DataBox> data) {
        return false;
    }

    @Override
    public boolean RequestReadAccess(long timestamp, List<DataBox> data, boolean[] is_ready) {
        return false;
    }

    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
    }

    @Override
    public void RequestAbort(long timestamp) {
    }

    @Override
    public SchemaRecord readValues(long ts, long previous_mark_ID, boolean clean) {
        SchemaRecord record = versions.get(ts);
        return record == null ? versions.lastEntry().getValue() : record;
    }

    @Override
    public void updateValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
        versions.putIfAbsent(ts, record);
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
    public void clean_map(long mark_ID) {
    }
}
