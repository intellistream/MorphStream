package intellistream.morphstream.engine.txn.transaction.impl;

import intellistream.morphstream.engine.txn.content.Content;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.record.SchemaRecordRef;
import intellistream.morphstream.engine.db.storage.impl.StorageManager;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * conventional two-phase locking with no-sync_ratio strategy from Cavalia.
 */
public class TxnManagerLock extends TxnManagerDedicatedLocked {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerLock.class);

    public TxnManagerLock(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
    }

    @Override
    public boolean InsertRecord(FunctionContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
//		BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record, (int) this.thread_count_);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            if (!tb_record.content_.TryWriteLock()) {
                this.AbortTransaction();
                return false;
            } else {
//				if (enable_log) LOG.info(tb_record.toString() + "is locked by insertor");
            }
            record.is_visible_ = true;
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = CommonMetaTypes.AccessType.INSERT_ONLY;
            access.access_record_ = tb_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = 0;
//		END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
            return true;
        } else {
//				END_PHASE_MEASURE(thread_id_, INSERT_PHASE);
            return true;
        }
    }

    @Override
    public boolean CommitTransaction(FunctionContext functionContext, int batchID) {
        // release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            TxnAccess.Access access_ptr = access_list_.GetAccess(i);
            if (access_ptr.access_type_ == CommonMetaTypes.AccessType.READ_ONLY) {
                access_ptr.access_record_.content_.ReleaseReadLock();
            } else if (access_ptr.access_type_ == CommonMetaTypes.AccessType.READ_WRITE) {
                SchemaRecord local_record_ptr = access_ptr.local_record_;
                access_ptr.access_record_.content_.ReleaseWriteLock();
                local_record_ptr.clean();
            } else {
                // insert_only or delete_only
                access_ptr.access_record_.content_.ReleaseWriteLock();
            }
        }
        assert (access_list_.access_count_ <= CommonMetaTypes.kMaxAccessNum);
        access_list_.Clear();
//		END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
        return true;
    }

    @Override
    public void AbortTransaction() {
        // recover updated data and release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            TxnAccess.Access access_ptr = access_list_.GetAccess(i);
            SchemaRecord local_record_ptr = access_ptr.local_record_;
            Content content_ref = access_ptr.access_record_.content_;
            if (access_ptr.access_type_ == CommonMetaTypes.AccessType.READ_ONLY) {
                content_ref.ReleaseReadLock();
            } else if (access_ptr.access_type_ == CommonMetaTypes.AccessType.READ_WRITE) {
//				access_ptr.access_record_.record_ = new SchemaRecord(local_record_ptr);
                content_ref.ReleaseWriteLock();
//				MemAllocator::Free(local_record_ptr->data_ptr_);
//				local_record_ptr->~SchemaRecord();
//				MemAllocator::Free((char*)local_record_ptr);
                local_record_ptr.clean();
                access_ptr.local_record_ = null;
            } else if (access_ptr.access_type_ == CommonMetaTypes.AccessType.INSERT_ONLY) {
                access_ptr.access_record_.record_.is_visible_ = false;
                content_ref.ReleaseWriteLock();
            } else if (access_ptr.access_type_ == CommonMetaTypes.AccessType.DELETE_ONLY) {
                access_ptr.access_record_.record_.is_visible_ = true;
                content_ref.ReleaseWriteLock();
            }
        }
        assert (access_list_.access_count_ <= CommonMetaTypes.kMaxAccessNum);
        access_list_.Clear();
    }

    @Override
    protected boolean SelectRecordCC(FunctionContext txn_context, String table_name, TableRecord
            t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_); //return the table record for modifying in the application layer.
        if (accessType == CommonMetaTypes.AccessType.READ_ONLY) {
            // if cannot get lock_ratio, then return immediately.
            if (!t_record.content_.TryReadLock()) {
                this.AbortTransaction();
                return false;
            } else {
                TxnAccess.Access access = access_list_.NewAccess();
                access.access_type_ = CommonMetaTypes.AccessType.READ_ONLY;
                access.access_record_ = t_record;
                access.local_record_ = null;
                access.table_id_ = table_name;
                access.timestamp_ = t_record.content_.GetTimestamp();
                return true;
            }
        } else if (accessType == CommonMetaTypes.AccessType.READ_WRITE) {
            if (!t_record.content_.TryWriteLock()) {
                if (enable_log)
                    if (enable_log)
                        LOG.info(Thread.currentThread().getName() + " failed to get lock_ratio" + DateTime.now());
                this.AbortTransaction();
                return false;
            } else {
                /**
                 * 	 const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
                 char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
                 SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
                 new(local_record)SchemaRecord(schema_ptr, local_data);
                 t_record->record_->CopyTo(local_record);
                 */
                final SchemaRecord local_record = new SchemaRecord(t_record.record_);//copy from t_record to local_record. This is kept for potential abort.
                /**
                 Access *access = access_list_.NewAccess();
                 access->access_type_ = READ_WRITE;
                 access->access_record_ = t_record;
                 access->local_record_ = local_record;
                 access->table_id_ = table_id;
                 access->timestamp_ = t_record->content_.GetTimestamp();
                 return true;
                 */
                TxnAccess.Access access = access_list_.NewAccess();
                access.access_type_ = CommonMetaTypes.AccessType.READ_WRITE;
                access.access_record_ = t_record;
                access.local_record_ = local_record;
                access.table_id_ = table_name;
                access.timestamp_ = t_record.content_.GetTimestamp();
                return true;
            }
        } else if (accessType == CommonMetaTypes.AccessType.DELETE_ONLY) {
            if (!t_record.content_.TryWriteLock()) {
                this.AbortTransaction();
                return false;
            } else {
                if (enable_log) LOG.info(t_record + "is locked by deletor");
                t_record.record_.is_visible_ = false;
                TxnAccess.Access access = access_list_.NewAccess();
                access.access_type_ = CommonMetaTypes.AccessType.DELETE_ONLY;
                access.access_record_ = t_record;
                access.local_record_ = null;
                access.table_id_ = table_name;
                access.timestamp_ = t_record.content_.GetTimestamp();
                return true;
            }
        } else {
            assert (false);
            return false;
        }
    }
}
