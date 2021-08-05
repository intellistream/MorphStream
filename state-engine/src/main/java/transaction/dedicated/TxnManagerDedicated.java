package transaction.dedicated;

import common.OrderLock;
import common.PartitionedOrderLock;
import common.meta.MetaTypes;
import common.meta.MetaTypes.AccessType;
import db.Database;
import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.*;
import storage.datatype.DataBox;
import transaction.TxnManager;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnAccess;
import transaction.impl.TxnContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;

import static common.meta.MetaTypes.kMaxAccessNum;

/**
 * TxnManagerDedicated is a thread-local structure.
 */
public abstract class TxnManagerDedicated implements TxnManager {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerDedicated.class);
    protected final StorageManager storageManager_;
    protected final String thisComponentId;
    private final long thread_id_;
    protected TxnAccess.AccessList access_list_ = new TxnAccess.AccessList(kMaxAccessNum);
    protected TableRecords t_records_ = new TableRecords(64);
    protected boolean is_first_access_;
    protected long start_timestamp_;
    protected long thread_count_;
    protected int delta;//range of each partition. depends on the number of op in the stage.
    private long local_epoch_;
    private long local_ts_;

    public TxnManagerDedicated(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        this.storageManager_ = storageManager;
        this.thisComponentId = thisComponentId;
        thread_id_ = thisTaskId;
        thread_count_ = thread_count;
        is_first_access_ = true;
    }

    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
    }

    public abstract boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException;

    public abstract void AbortTransaction();

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

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, List<DataBox> value, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.WRITE_ONLY;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_WriteRecordCC(txn_context, srcTable, t_record, primary_key, value, enqueue_time, accessType);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_WriteRecord(TxnContext txn_context, String srcTable, String primary_key, long value, int column_id) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.WRITE_ONLY;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_WriteRecordCC(txn_context, primary_key, srcTable, t_record, value, column_id, accessType);//TxnContext txn_context, String srcTable, String primary_key, long value_list, int column_id
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    public boolean Asy_ReadRecord(TxnContext txn_context, String srcTable, String primary_key, SchemaRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_ONLY;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ReadRecordCC(txn_context, primary_key, srcTable, t_record, record_ref, enqueue_time, accessType);
        } else {
            // if no record_ref is found, then a "virtual record_ref" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ReadRecords(TxnContext txn_context, String srcTable, String primary_key, TableRecordRef record_ref, double[] enqueue_time) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READS_ONLY;//read multiple versions.
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(primary_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ReadRecordCC(txn_context, primary_key, srcTable, t_record, record_ref, enqueue_time, accessType);
        } else {
            // if no record_ref is found, then a "virtual record_ref" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, Function function, int column_id) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, t_record, t_record, function, accessType, column_id);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + source_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String source_key, String dest_key, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
        TableRecord d_record = storageManager_.getTable(srcTable).SelectKeyRecord(dest_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, t_record, d_record, function, accessType, 1);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + source_key);
            return false;
        }
    }

    // Modify for deposit
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE;
//        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
//        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, t_record, t_record, function, accessType, 1);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String source_key, String dest_key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE_READ;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(source_key);
        TableRecord d_record = storageManager_.getTable(srcTable).SelectKeyRecord(dest_key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ModifyRecord_ReadCC(txn_context, srcTable, t_record, d_record, record_ref, function, accessType);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + source_key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref, Function function) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE_READ;
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            return Asy_ModifyRecord_ReadCC(txn_context, srcTable, t_record, record_ref, function, accessType);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + key);
            return false;
        }
    }

    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String src_key, String dest_key, Function function, String[] condition_sourceTable, String[] condition_source, Condition condition, boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(src_key);
        TableRecord d_record = storageManager_.getTable(srcTable).SelectKeyRecord(dest_key);
        if (d_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, src_key, s_record, d_record, function, condition_sourceTable, condition_source, condition_records, condition, accessType, success);
        } else {
            LOG.info("No record is found:" + src_key);
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }

    /**
     * condition on itself.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param function
     * @param condition
     * @param success
     * @return
     * @throws DatabaseException
     */
    @Override
    public boolean Asy_ModifyRecord(TxnContext txn_context, String srcTable, String key, Function function, Condition condition, boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[1];
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        condition_records[0] = s_record;
        if (s_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, key, s_record, function, new String[]{srcTable}, new String[]{key}, condition_records, condition, accessType, success);
        } else {
            LOG.info("No record is found:" + key);
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }

    /**
     * condition on others.
     *
     * @param txn_context
     * @param srcTable
     * @param key
     * @param function
     * @param condition_sourceTable
     * @param condition_source
     * @param condition
     * @param success
     * @return
     * @throws DatabaseException
     */

//     transactionManager.Asy_ModifyRecord(txnContext,
//            "bookEntries", event.getSourceBookEntryId()
//            , new DEC(event.getBookEntryTransfer())
//            , srcTable, srcID,
//            new Condition(event.getMinAccountBalance(), event.getAccountTransfer(), event.getBookEntryTransfer()),
//    event.success);   //asynchronously return.
    @Override // TRANSFER_AST
    public boolean Asy_ModifyRecord(TxnContext txn_context,
                                    String srcTable, String key,
                                    Function function,
                                    String[] condition_sourceTable, String[] condition_source,
                                    Condition condition,
                                    boolean[] success) throws DatabaseException {
        MetaTypes.AccessType accessType = AccessType.READ_WRITE_COND;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            return Asy_ModifyRecordCC(txn_context, srcTable, key, s_record, function, condition_sourceTable, condition_source, condition_records, condition, accessType, success);
        } else {
            LOG.info("No record is found:" + key);
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            return false;
        }
    }

    @Override // TRANSFER_ACT
    public boolean Asy_ModifyRecord_Read(TxnContext txn_context, String srcTable, String key, SchemaRecordRef record_ref,
                                         Function function,
                                         String[] condition_sourceTable, String[] condition_source,
                                         Condition condition, boolean[] success) throws DatabaseException {

        MetaTypes.AccessType accessType = AccessType.READ_WRITE_COND_READ;
        TableRecord[] condition_records = new TableRecord[condition_source.length];
        for (int i = 0; i < condition_source.length; i++) {
            condition_records[i] = storageManager_.getTable(condition_sourceTable[i]).SelectKeyRecord(condition_source[i]);//TODO: improve this later.
            if (condition_records[i] == null) {
                LOG.info("No record is found for condition source:" + condition_source[i]);
                return false;
            }
        }
        TableRecord s_record = storageManager_.getTable(srcTable).SelectKeyRecord(key);
        if (s_record != null) {
            return Asy_ModifyRecord_ReadCC(txn_context, srcTable, key, s_record, record_ref, function, condition_sourceTable, condition_source, condition_records, condition, accessType, success);
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + key);
            return false;
        }
    }

    public void start_evaluate(int taskId, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    /**
     * Single record_ selection query. This is shared by all TM.
     *
     * @param txn_context
     * @param table_name
     * @param primary_key
     * @param record_
     * @param access_type
     * @return
     */
    public boolean SelectKeyRecord(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, AccessType access_type) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);//index look up.
        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
//
        if (t_record != null) {
//			BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
            boolean rt = SelectRecordCC(txn_context, table_name, t_record, record_, access_type);
//			END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
            assert !rt || record_.getRecord() != null;
            return rt;
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    public boolean lock_ahead(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, AccessType access_type) throws DatabaseException {
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);
        if (t_record != null) {
            boolean rt = lock_aheadCC(txn_context, table_name, t_record, record_, access_type);
            return rt;
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
            return false;
        }
    }

    public boolean SelectKeyRecord_noLock(TxnContext txn_context, String table_name, String primary_key, SchemaRecordRef record_, AccessType access_type) throws DatabaseException {
        MeasureTools.BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        TableRecord t_record = storageManager_.getTable(table_name).SelectKeyRecord(primary_key);
        MeasureTools.END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
        if (t_record != null) {
            boolean rt = SelectKeyRecord_noLockCC(txn_context, table_name, t_record, record_, access_type);
            return rt;
        } else {
            // if no record_ is found, then a "virtual record_" should be inserted as the placeholder so that we can lock_ratio it.
            LOG.info("No record is found:" + primary_key);
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
    public boolean SelectRecords(TxnContext txn_context, String table_name, int idx_id, String secondary_key, SchemaRecords records_, AccessType access_type, LinkedList<Long> gap) throws DatabaseException, InterruptedException {
//        BEGIN_INDEX_TIME_MEASURE(txn_context.thread_Id);
        storageManager_.getTable(table_name).SelectRecords(idx_id, secondary_key, t_records_);
//        END_INDEX_TIME_MEASURE_ACC(txn_context.thread_Id, txn_context.is_retry_);
//		BEGIN_PHASE_MEASURE(thread_id_, SELECT_PHASE);
        SelectRecordsCC(txn_context, table_name, t_records_, records_, access_type, gap);
        t_records_.Clear();
//		END_PHASE_MEASURE(thread_id_, SELECT_PHASE);
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
    protected boolean SelectRecordsCC(TxnContext txn_context, String table_name, TableRecords t_records, SchemaRecords records_, AccessType access_type, LinkedList<Long> gap) throws InterruptedException {
        /**
         * 		bool SelectRecordsCC(TxnContext *context, const size_t &table_id, TableRecords *t_records, SchemaRecords *records, const AccessType access_type){
         *		for (size_t i = 0; i < t_records->curr_size_; ++i) {
         * 			SchemaRecord **s_record = &(records->records_[i]);
         *			TableRecord *t_record = t_records->records_[i];
         * 			if (SelectRecordCC(context, table_id, t_record, *s_record, access_type) == false) {
         *				return false;
         *                }
         *            }
         *			return true;
         *        }
         */
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

    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, SchemaRecordRef record_ref, double[] enqueue_time, AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ReadRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, TableRecordRef record_ref, double[] enqueue_time, AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    //txn_context, srcTable, t_record, value_list, accessType, column_id
    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String primary_key, String table_name, TableRecord t_record, long value, int column_id, AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_WriteRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, String primary_key, List<DataBox> value, double[] enqueue_time, AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord t_record, TableRecord d_record, Function function, AccessType accessType, int column_id) {
        throw new UnsupportedOperationException();
    }

    /**
     * With conditions.
     *
     * @param txn_context
     * @param srcTable
     * @param s_record
     * @param record_ref
     * @param function
     * @param condition_records
     * @param condition
     * @param accessType
     * @param success
     * @return
     */
    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, String sourceKey, TableRecord s_record, SchemaRecordRef record_ref, Function function,
                                              String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, AccessType accessType, boolean[] success) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord t_record, SchemaRecordRef record_ref, Function function, AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ModifyRecord_ReadCC(TxnContext txn_context, String srcTable, TableRecord s_record, TableRecord t_record, SchemaRecordRef record_ref, Function function, AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, TableRecord t_record, Function function, AccessType accessType) {
        return Asy_ModifyRecordCC(txn_context, srcTable, t_record, t_record, function, accessType, 1);
    }

    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, String sourceKey, TableRecord s_record, TableRecord d_record, Function function,
                                         String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, AccessType accessType, boolean[] success) {
        throw new UnsupportedOperationException();
    }

    protected boolean Asy_ModifyRecordCC(TxnContext txn_context, String srcTable, String sourceKey, TableRecord s_record, Function function,
                                         String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, AccessType accessType, boolean[] success) {
        return Asy_ModifyRecordCC(txn_context, srcTable, sourceKey, s_record, s_record, function, condition_sourceTable, condition_source, condition_records, condition, accessType, success);
    }

    protected abstract boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, AccessType access_type) throws InterruptedException;

    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    protected boolean lock_aheadCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, AccessType access_type) {
        throw new UnsupportedOperationException();
    }

    public abstract boolean CommitTransaction(TxnContext txn_context);

    @Override
    public boolean SelectRecords(Database db, TxnContext txn_context, String table_name, int i, String secondary_key, SchemaRecords records, MetaTypes.AccessType accessType, LinkedList<Long> gap) {
        throw new UnsupportedOperationException();
    }
}
