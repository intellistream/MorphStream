package transaction.dedicated.ordered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import db.DatabaseException;
import common.meta.MetaTypes;
import common.OrderValidate;
import content.Content;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.dedicated.TxnManagerDedicated;
import transaction.impl.Epoch;
import transaction.impl.GlobalTimestamp;
import transaction.impl.TxnContext;

import java.util.LinkedList;

import static common.meta.MetaTypes.AccessType.*;
import static common.meta.MetaTypes.kMaxAccessNum;
import static transaction.impl.TxnAccess.Access;
/**
 * ordering constrained occ. in-completed
 */
public class TxnManagerOrderedOcc extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerOrderedOcc.class);
    final OrderValidate orderValidate;
    public TxnManagerOrderedOcc(StorageManager storageManager, OrderValidate orderValidate, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
        this.orderValidate = orderValidate;
    }
    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            record.is_visible_ = true;
            Access access = access_list_.NewAccess();
            access.access_type_ = INSERT_ONLY;
            access.access_record_ = tb_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = 0;
            return true;
        } else {
            return true;
        }
    }
    @Override
    public boolean CommitTransaction(TxnContext txnContext) {
        // step 1: acquire lock_ratio and validate
        int lock_count = 0;
        boolean is_success = true;
        access_list_.Sort();
        for (int i = 0; i < access_list_.access_count_; ++i) {
            ++lock_count;
            Access access_ptr = access_list_.GetAccess(i);
            Content content_ref = access_ptr.access_record_.content_;
            if (access_ptr.access_type_ == READ_ONLY) {
                // acquire read lock_ratio
                content_ref.AcquireReadLock();//make sure no one read it while I'm doing validation.
                // whether someone has changed the tuple after my read
                if (content_ref.GetTimestamp() != access_ptr.timestamp_) {
//					UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
                    is_success = false;
                    break;
                }
            } else if (access_ptr.access_type_ == READ_WRITE) {
                // acquire write lock_ratio
                content_ref.AcquireWriteLock();//make sure no one read/write it while I'm doing validation.
                // whether someone has changed the tuple after my read
                if (content_ref.GetTimestamp() != access_ptr.timestamp_) {
//					UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
                    is_success = false;
                    break;
                }
            } else {
                // insert_only or delete_only
                content_ref.AcquireWriteLock();
            }
        }
        // Additional step: validate ordering.
        if (!orderValidate.validate(txnContext.getBID())) {
            is_success = false;
        }
        // step 2: if success, then overwrite and commit
        if (is_success) {
//				BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
            long curr_epoch = Epoch.GetEpoch();
            long commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp.GetMonotoneTimestamp());
//				END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                SchemaRecord global_record_ptr = access_ptr.access_record_.record_;
                SchemaRecord local_record_ptr = access_ptr.local_record_;
                Content content_ref = access_ptr.access_record_.content_;
                if (access_ptr.access_type_ == READ_WRITE) {
                    assert (commit_ts > access_ptr.timestamp_);
                    assert global_record_ptr != null;
                    global_record_ptr.CopyFrom(local_record_ptr);//double check..
//					COMPILER_MEMORY_FENCE;
                    content_ref.SetTimestamp(commit_ts);
                } else if (access_ptr.access_type_ == DELETE_ONLY) {
                    assert (commit_ts > access_ptr.timestamp_);
                    global_record_ptr.is_visible_ = false;
//					COMPILER_MEMORY_FENCE;
                    content_ref.SetTimestamp(commit_ts);
                }
            }
            // commit.
            //TODO: LOG disabled..
            // step 3: release locks and clean up.
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                if (access_ptr.access_type_ == READ_ONLY) {
                    access_ptr.access_record_.content_.ReleaseReadLock();
                } else if (access_ptr.access_type_ == READ_WRITE) {
                    access_ptr.access_record_.content_.ReleaseWriteLock();
//					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
                    SchemaRecord local_record_ptr = access_ptr.local_record_;
                    local_record_ptr.clean();
                    access_ptr.local_record_ = null;
                } else {
                    // insert_only or delete_only
                    access_ptr.access_record_.content_.ReleaseWriteLock();
                }
            }
            // Additional step: validate ordering advance.
            orderValidate.advance();
        }
        // if failed.
        else {
            // step 3: release locks and clean up.
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                if (access_ptr.access_type_ == READ_ONLY) {
                    access_ptr.access_record_.content_.ReleaseReadLock();
                } else if (access_ptr.access_type_ == READ_WRITE) {
                    SchemaRecord local_record_ptr = access_ptr.local_record_;
                    access_ptr.access_record_.content_.ReleaseWriteLock();
                    local_record_ptr.clean();
                    access_ptr.local_record_ = null;
                } else {
                    // insert_only or delete_only
                    access_ptr.access_record_.content_.ReleaseWriteLock();
                }
                --lock_count;
                if (lock_count == 0) {
                    break;
                }
            }
        }
        assert (access_list_.access_count_ <= kMaxAccessNum);
//		END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
        access_list_.Clear();
        return is_success;
    }
    @Override
    public void AbortTransaction() {
        assert (false);
    }
    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef s_record_ref, MetaTypes.AccessType accessType) {
        if (accessType == READ_ONLY) {
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            s_record_ref.setRecord(t_record.record_);
            return true;
        } else if (accessType == READ_WRITE) {
            /**
             * 	 Access *access = access_list_.NewAccess();
             access->access_type_ = READ_WRITE;
             access->access_record_ = t_record;
             */
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
            /** copy data
             BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
             const RecordSchema *schema_ptr = t_record->record_->schema_ptr_;
             char *local_data = MemAllocator::Alloc(schema_ptr->GetSchemaSize());
             SchemaRecord *local_record = (SchemaRecord*)MemAllocator::Alloc(sizeof(SchemaRecord));
             new(local_record)SchemaRecord(schema_ptr, local_data);
             END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
             */
            final SchemaRecord local_record = new SchemaRecord(t_record.record_);//copy from t_record to local_record.
            /**
             access->timestamp_ = t_record->content_.GetTimestamp();
             COMPILER_MEMORY_FENCE;
             local_record->CopyFrom(t_record->record_);
             access->local_record_ = local_record;
             access->table_id_ = table_id;
             */
            access.timestamp_ = t_record.content_.GetTimestamp();
            //memory fence? why is it needed?
            access.local_record_ = local_record;
            access.table_id_ = table_name;
            /**
             *  // reset returned d_record.
             s_record = local_record;
             return true;
             */
            s_record_ref.setRecord(local_record);
            return true;
        } else if (accessType == DELETE_ONLY) {
            LOG.info(t_record.toString() + "is locked by deleter");
            t_record.record_.is_visible_ = false;
            Access access = access_list_.NewAccess();
            access.access_type_ = DELETE_ONLY;
            access.access_record_ = t_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            s_record_ref.setRecord(t_record.record_);
            return true;
        } else {
            assert (false);
            return false;
        }
    }
}
