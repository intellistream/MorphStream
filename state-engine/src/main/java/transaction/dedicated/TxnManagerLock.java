package transaction.dedicated;

import common.meta.CommonMetaTypes;
import content.Content;
import db.DatabaseException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.impl.TxnContext;

import java.util.LinkedList;

import static common.CONTROL.enable_debug;
import static common.meta.CommonMetaTypes.AccessType.*;
import static common.meta.CommonMetaTypes.kMaxAccessNum;
import static transaction.impl.TxnAccess.Access;

/**
 * conventional two-phase locking with no-sync_ratio strategy from Cavalia.
 */
public class TxnManagerLock extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerLock.class);

    public TxnManagerLock(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
//		BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            if (!tb_record.content_.TryWriteLock()) {
                this.AbortTransaction();
                return false;
            } else {
//				LOG.info(tb_record.toString() + "is locked by insertor");
            }
            record.is_visible_ = true;
            Access access = access_list_.NewAccess();
            access.access_type_ = INSERT_ONLY;
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
    public boolean CommitTransaction(TxnContext txnContext) {
        // release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            Access access_ptr = access_list_.GetAccess(i);
            if (access_ptr.access_type_ == READ_ONLY) {
                access_ptr.access_record_.content_.ReleaseReadLock();
            } else if (access_ptr.access_type_ == READ_WRITE) {
                SchemaRecord local_record_ptr = access_ptr.local_record_;
                access_ptr.access_record_.content_.ReleaseWriteLock();
                local_record_ptr.clean();
            } else {
                // insert_only or delete_only
                access_ptr.access_record_.content_.ReleaseWriteLock();
            }
        }
        assert (access_list_.access_count_ <= kMaxAccessNum);
        access_list_.Clear();
//		END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
        return true;
    }

    @Override
    public void AbortTransaction() {
        // recover updated data and release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            Access access_ptr = access_list_.GetAccess(i);
            SchemaRecord local_record_ptr = access_ptr.local_record_;
            Content content_ref = access_ptr.access_record_.content_;
            if (access_ptr.access_type_ == READ_ONLY) {
                content_ref.ReleaseReadLock();
            } else if (access_ptr.access_type_ == READ_WRITE) {
//				access_ptr.access_record_.record_ = new SchemaRecord(local_record_ptr);
                content_ref.ReleaseWriteLock();
//				MemAllocator::Free(local_record_ptr->data_ptr_);
//				local_record_ptr->~SchemaRecord();
//				MemAllocator::Free((char*)local_record_ptr);
                local_record_ptr.clean();
                access_ptr.local_record_ = null;
            } else if (access_ptr.access_type_ == INSERT_ONLY) {
                access_ptr.access_record_.record_.is_visible_ = false;
                content_ref.ReleaseWriteLock();
            } else if (access_ptr.access_type_ == DELETE_ONLY) {
                access_ptr.access_record_.record_.is_visible_ = true;
                content_ref.ReleaseWriteLock();
            }
        }
        assert (access_list_.access_count_ <= kMaxAccessNum);
        access_list_.Clear();
    }

    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord
            t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_); //return the table record for modifying in the application layer.
        if (accessType == READ_ONLY) {
            // if cannot get lock_ratio, then return immediately.
            if (!t_record.content_.TryReadLock()) {
                this.AbortTransaction();
                return false;
            } else {
                Access access = access_list_.NewAccess();
                access.access_type_ = READ_ONLY;
                access.access_record_ = t_record;
                access.local_record_ = null;
                access.table_id_ = table_name;
                access.timestamp_ = t_record.content_.GetTimestamp();
                return true;
            }
        } else if (accessType == READ_WRITE) {
            if (!t_record.content_.TryWriteLock()) {
                if (enable_debug)
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
                Access access = access_list_.NewAccess();
                access.access_type_ = READ_WRITE;
                access.access_record_ = t_record;
                access.local_record_ = local_record;
                access.table_id_ = table_name;
                access.timestamp_ = t_record.content_.GetTimestamp();
                return true;
            }
        } else if (accessType == DELETE_ONLY) {
            if (!t_record.content_.TryWriteLock()) {
                this.AbortTransaction();
                return false;
            } else {
                LOG.info(t_record.toString() + "is locked by deletor");
                t_record.record_.is_visible_ = false;
                Access access = access_list_.NewAccess();
                access.access_type_ = DELETE_ONLY;
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
