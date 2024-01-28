package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.content.common.ContentCommon;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeSet;

/**
 * This corresponds to ACEP's SharedTable, but for every table d_record.
 */
public class LWMContentImpl extends LWMContent {
    public final static String LWM_CONTENT = "LWM_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(LWMContentImpl.class);
    SpinLock wait_lock_ = new SpinLock();
    volatile boolean is_certifying_ = false;
    volatile long read_count_;
    volatile boolean is_writing_ = false;
    volatile TreeSet<Long> set = new TreeSet<>();
    volatile long lwm = Long.MAX_VALUE;

    public LWMContentImpl() {
        set.add(lwm);
    }

    @Override
    public boolean AcquireReadLock() {
        boolean rt = true;
        wait_lock_.lock();
        if (is_certifying_) {
            rt = false;
        } else {
            ++read_count_;
        }
        wait_lock_.unlock();
        return rt;
    }

    /**
     * Read lock_ratio will not block write.. --> major difference to S2PL.
     * Write will still prevent Write.. --> multiple write to a d_record is not allowed.
     *
     * @return
     */
    @Override
    public boolean AcquireWriteLock() {
        boolean rt = true;
        wait_lock_.lock();
        if (is_writing_ || is_certifying_) {
            rt = false;
        } else {
            is_writing_ = true;
        }
        wait_lock_.unlock();
        return rt;
    }

    @Override
    public void ReleaseReadLock() {
        wait_lock_.lock();
        assert (read_count_ > 0);
        --read_count_;
        wait_lock_.unlock();
    }

    @Override
    public void ReleaseWriteLock() {
        wait_lock_.lock();
        assert (is_writing_);
        is_writing_ = false;
        wait_lock_.unlock();
    }

    @Override
    public long GetLWM() {
        return lwm;
    }

    @Override
    public SchemaRecord ReadAccess(FunctionContext txn_context, CommonMetaTypes.AccessType accessType) {
        int retry_count = 0;
        long bid = txn_context.getBID();
        switch (accessType) {
            case READ_ONLY:
                while (bid > GetLWM() && !Thread.currentThread().isInterrupted()) {
//					retry_count++;
//					if (retry_count > 100) {
//						if (enable_log) LOG.error("Retry:" + retry_count + " ts: " + ts + " lwm:" + lwm);
//					}
                }
                break;
            case READ_WRITE:
                while (bid != GetLWM() && !Thread.currentThread().isInterrupted()) {
//					retry_count++;
//					if (retry_count > 100) {
//						if (enable_log) LOG.error("Retry:" + retry_count + " ts: " + ts + " lwm:" + lwm);
//					}
                }
                break;
        }
        //TODO: there is a null pointer error at this line.
//        BEGIN_TP_CORE_TIME_MEASURE(txn_context.thread_Id);
        SchemaRecord record = readValues(bid, -1, false);
//        END_TP_CORE_TIME_MEASURE_TS(txn_context.thread_Id, 1);
        return record;
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
    public void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
    }

    //However, once T is ready to commit, it must obtain a certify lock_ratio on all items that it currently holds write locks on before it can commit.
    @Override
    public boolean AcquireCertifyLock() {
        boolean rt = true;
        wait_lock_.lock();
        assert (is_writing_);
        assert (!is_certifying_);
        if (read_count_ != 0) {
            rt = false;
        } else {
            is_writing_ = false;
            is_certifying_ = true;
        }
        wait_lock_.unlock();
        return rt;
    }

    @Override
    public void WriteAccess(long commit_timestamp, long mark_ID, boolean clean, SchemaRecord local_record_) {
        rw_lock_.AcquireWriteLock();
        updateValues(commit_timestamp, mark_ID, clean, local_record_);
        CollectGarbage();
        rw_lock_.ReleaseWriteLock();
    }

    private void CollectGarbage() {
        if (versions.size() > ContentCommon.kRecycleLength) {
            ClearHistory(lwm);
        }
    }

    private void ClearHistory(long min_thread_ts) {
//		versions.clear();
        versions.headMap(min_thread_ts).clear();
    }

    @Override
    public void ReleaseCertifyLock() {
        wait_lock_.lock();
        assert (is_certifying_);
        is_certifying_ = false;
        wait_lock_.unlock();
    }

    private void MaintainLWM() {
        lwm = set.first();
    }

    @Override
    public synchronized void AddLWM(long ts) {
        set.add(ts);
        MaintainLWM();
    }

    @Override
    public synchronized void DeleteLWM(long ts) {
        set.remove(ts);
        MaintainLWM();
    }
}
