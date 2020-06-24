package state_engine.transaction.dedicated;

import state_engine.DatabaseException;
import state_engine.Meta.MetaTypes;
import state_engine.content.Content;
import state_engine.storage.SchemaRecord;
import state_engine.storage.SchemaRecordRef;
import state_engine.storage.StorageManager;
import state_engine.storage.TableRecord;
import state_engine.transaction.impl.Epoch;
import state_engine.transaction.impl.GlobalTimestamp;
import state_engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static state_engine.Meta.MetaTypes.AccessType.*;
import static state_engine.Meta.MetaTypes.kMaxAccessNum;
import static state_engine.transaction.impl.TxnAccess.Access;

/**
 * Conventional occ from Cavalia.
 */
public class TxnManagerOcc extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerOcc.class);

    public TxnManagerOcc(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);

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
                content_ref.AcquireReadLock();
                // whether someone has changed the tuple after my read
                if (content_ref.GetTimestamp() != access_ptr.timestamp_) {
//					UPDATE_CC_ABORT_COUNT(thread_id_, context->txn_type_, access_ptr->table_id_);
                    is_success = false;
                    break;
                }
            } else if (access_ptr.access_type_ == READ_WRITE) {
                // acquire write lock_ratio
                content_ref.AcquireWriteLock();
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

        /**
         * // step 2: if success, then overwrite and commit
         * if (is_success == true) {
         * 	BEGIN_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
         * 	uint64_t curr_epoch = Epoch::GetEpoch();
         * 	#if defined(SCALABLE_TIMESTAMP)
         * 		uint64_t max_rw_ts = 0;
         for (size_t i = 0; i < access_list_.access_count_; ++i){
         Access *access_ptr = access_list_.GetAccess(i);
         if (access_ptr->timestamp_ > max_rw_ts){
         max_rw_ts = access_ptr->timestamp_;
         }
         }
         uint64_t commit_ts = GenerateScalableTimestamp(curr_epoch, max_rw_ts);
         *	#else
         *	uint64_t commit_ts = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp::GetMonotoneTimestamp());
         *	#endif
         *	END_CC_TS_ALLOC_TIME_MEASURE(thread_id_);
         *	for (size_t i = 0; i < access_list_.access_count_; ++i) {
         *    	Access *access_ptr = access_list_.GetAccess(i);
         *		SchemaRecord *global_record_ptr = access_ptr->access_record_->record_;
         *	    SchemaRecord *local_record_ptr = access_ptr->local_record_;
         *	    auto &content_ref = access_ptr->access_record_->content_;
         *	    if (access_ptr->access_type_ == READ_WRITE) {
         *			assert(commit_ts > access_ptr->timestamp_);
         *			COMPILER_MEMORY_FENCE;
         *			content_ref.SetTimestamp(commit_ts);
         *        }
         *        else if (access_ptr->access_type_ == DELETE_ONLY) {
         *        	assert(commit_ts > access_ptr->timestamp_);
         *        	global_record_ptr->is_visible_ = false;
         *        	COMPILER_MEMORY_FENCE;
         *        	content_ref.SetTimestamp(commit_ts);
         *        }
         *    }
         * }
         */

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


            /**
             * // commit.
             * #if defined(VALUE_LOGGING)
             *	 logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
             *	 #elif defined(COMMAND_LOGGING)
             *	 if (context->is_adhoc_ == true){
             *	 logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, access_list_);
             *     }
             *	 logger_->CommitTransaction(this->thread_id_, curr_epoch, commit_ts, context->txn_type_, param);
             *	 #endif
             * 	 // step 3: release locks and clean up.
             * 	 for (size_t i = 0; i < access_list_.access_count_; ++i) {
             * 	 	Access *access_ptr = access_list_.GetAccess(i);
             *	 	if (access_ptr->access_type_ == READ_ONLY) {
             *	 	access_ptr->access_record_->content_.ReleaseReadLock();
             *     }
             *	 else if (access_ptr->access_type_ == READ_WRITE) {
             *	 	access_ptr->access_record_->content_.ReleaseWriteLock();
             *	 	BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
             *	 	SchemaRecord *local_record_ptr = access_ptr->local_record_;
             *	 	MemAllocator::Free(local_record_ptr->data_ptr_);
             *	 	local_record_ptr->~SchemaRecord();
             *	 	MemAllocator::Free((char*)local_record_ptr);
             *		END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
             *    }
             *    else{
             *    // insert_only or delete_only
             *		access_ptr->access_record_->content_.ReleaseWriteLock();
             *    }
             *
             *
             *
             */
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
//					BEGIN_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
//					MemAllocator::Free(local_record_ptr->data_ptr_);
//					local_record_ptr->~SchemaRecord();
//					MemAllocator::Free((char*)local_record_ptr);
//					END_CC_MEM_ALLOC_TIME_MEASURE(thread_id_);
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
