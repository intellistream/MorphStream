package intellistream.morphstream.engine.txn.transaction.impl;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSchedulerContext;
import intellistream.morphstream.engine.txn.storage.*;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * TxnManagerDedicated is a thread-local structure.
 */
public abstract class TxnManagerDedicatedLocked extends TxnManager {
    private static final Logger log = LoggerFactory.getLogger(TxnManagerDedicatedLocked.class);
    protected final String thisComponentId;
    private final long thread_id_;
    protected TxnAccess.AccessList access_list_ = new TxnAccess.AccessList(CommonMetaTypes.kMaxAccessNum);
    protected TableRecords t_records_ = new TableRecords(64);
    protected boolean is_first_access_;
    protected long start_timestamp_;
    protected long thread_count_;
    private long local_epoch_;
    private long local_ts_;

    public TxnManagerDedicatedLocked(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        this.storageManager_ = storageManager;
        this.thisComponentId = thisComponentId;
        thread_id_ = thisTaskId;
        thread_count_ = thread_count;
        is_first_access_ = true;
    }

    public long GenerateScalableTimestamp(long curr_epoch, long max_rw_ts) {
        long max_global_ts = max_rw_ts >> 32;
        long max_local_ts = max_rw_ts & 0xFFFFFFFF;
        assert (curr_epoch >= max_global_ts);
        assert (curr_epoch >= this.local_epoch_);
        // init.
        if (curr_epoch > this.local_epoch_) {
            this.local_epoch_ = curr_epoch;
            this.local_ts_ = this.thread_id_;
        }
        assert (this.local_epoch_ == curr_epoch);
        // compute commit timestamp.
        if (curr_epoch == max_global_ts) {
            if (this.local_ts_ <= max_local_ts) {
                this.local_ts_ = (max_local_ts / thread_count_ + 1) * thread_count_ + thread_id_;
                assert (this.local_ts_ > max_local_ts);
            }
            assert (this.local_ts_ > max_local_ts);
        }
        assert (this.local_epoch_ == max_global_ts && this.local_ts_ >= max_local_ts || this.
                local_epoch_ > max_global_ts);
        long commit_ts = (this.local_epoch_ << 32) | this.local_ts_;
        assert (commit_ts >= max_rw_ts);
        return commit_ts;
    }

    protected long GenerateMonotoneTimestamp(long curr_epoch, long monotone_ts) {
		/*	uint32_t lower_bits = monotone_ts & 0xFFFFFFFF;
			uint64_t commit_ts = (curr_epoch << 32) | lower_bits;
		*/
        long lower_bits = monotone_ts & 0xFFFFFFFF;
        long commit_ts = (curr_epoch << 32) | lower_bits;
        return commit_ts;
    }

    public void start_evaluate(int taskId, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    public void AbortTransaction() {
        throw new UnsupportedOperationException();
    }

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
    }

    public abstract boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    @Override
    public boolean SelectKeyRecord(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, CommonMetaTypes.AccessType access_type) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);//index look up.
        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            boolean rt = SelectRecordCC(txn_context, table_name, t_record, record_, access_type);
            assert !rt || record_.getRecord() != null;
            return rt;
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean lock_ahead(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, CommonMetaTypes.AccessType access_type) throws DatabaseException {
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);
        if (t_record != null) {
            boolean rt = lock_aheadCC(txn_context, table_name, t_record, record_, access_type);
            return rt;
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, CommonMetaTypes.AccessType access_type) throws DatabaseException {
        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);
        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            boolean rt = SelectKeyRecord_noLockCC(txn_context, table_name, t_record, record_, access_type);
            return rt;
        } else {
            if (enable_log) log.info("No record is found:" + primary_key);
            return false;
        }
    }

    /**
     * @param txn_context
     * @param table_name
     * @param idx_id
     * @param secondary_key
     * @param records_      to be set.
     * @param access_type
     * @return
     * @throws DatabaseException
     */
    public boolean SelectRecords(TxnContext txn_context, String table_name, int idx_id, String secondary_key, SchemaRecords records_, CommonMetaTypes.AccessType access_type, LinkedList<Long> gap) throws DatabaseException, InterruptedException {
        storageManager_.getTable(table_name).SelectRecords(idx_id, secondary_key, t_records_);
        SelectRecordsCC(txn_context, table_name, t_records_, records_, access_type, gap);
        t_records_.Clear();
        return true;
    }

    /**
     * @param txn_context
     * @param table_name
     * @param t_records
     * @param records_    to be set.
     * @param access_type
     * @return
     */
    protected boolean SelectRecordsCC(TxnContext txn_context, String table_name, TableRecords t_records, SchemaRecords records_, CommonMetaTypes.AccessType access_type, LinkedList<Long> gap) throws InterruptedException {
        for (int i = 0; i < t_records.curr_size_; ++i) {
            SchemaRecordRef record_ref = new SchemaRecordRef();
            if (!SelectRecordCC(txn_context, table_name, t_records.records_[i], record_ref, access_type)) {
                return false;
            } else {
                records_.setRecords_(i, record_ref.getRecord());
            }
        }
        return true;
    }

    protected abstract boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType access_type) throws InterruptedException;

    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    protected boolean lock_aheadCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void BeginTransaction(TxnContext txn_context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public abstract boolean CommitTransaction(TxnContext txn_context);

    @Override
    public boolean submitStateAccess(StateAccess stateAccess, TxnContext txnContext) {
        throw new UnsupportedOperationException();
    }

    // Those should not be used by dedicated locked txn manager.
    @Override
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String key, StateAccess stateAccess) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String table, String key, StateAccess stateAccess) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_WriteRecord_Cond(TxnContext txn_context, String srcTable, String key, String[] condition_sourceTable, String[] condition_source, StateAccess stateAccess) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_WindowReadRecords(TxnContext txn_context, String srcTable, String key, String[] condition_sourceTable, String[] condition_source) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Asy_WriteRecord_Non_Deter(TxnContext txn_context, String srcTable, String key, String[] condition_sourceTable, String[] condition_source, StateAccess stateAccess) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }


    @Override
    public OGSchedulerContext getSchedulerContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void switch_scheduler(int thread_Id, long mark_ID) {
        throw new UnsupportedOperationException();
    }
}
