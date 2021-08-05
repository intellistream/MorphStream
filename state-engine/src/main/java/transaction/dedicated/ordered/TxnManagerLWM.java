package transaction.dedicated.ordered;

import common.OrderLock;
import common.meta.CommonMetaTypes;
import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.dedicated.TxnManagerDedicated;
import transaction.impl.TxnContext;

import java.util.LinkedList;

import static common.meta.CommonMetaTypes.AccessType.*;
import static common.meta.CommonMetaTypes.kMaxAccessNum;
import static transaction.impl.TxnAccess.Access;

/**
 * mimic of ACEP's LWM method.
 */
public class TxnManagerLWM extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerLWM.class);
    final OrderLock orderLock;

    public TxnManagerLWM(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
        this.orderLock = OrderLock.getInstance();
    }

    public OrderLock getOrderLock() {
        return orderLock;
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap)
            throws DatabaseException, InterruptedException {
//		BEGIN_PHASE_MEASURE(thread_id_, INSERT_PHASE);
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            orderLock.blocking_wait(txn_context.getBID());
            while (!tb_record.content_.TryWriteLock() && !Thread.currentThread().isInterrupted()) {//order guaranteed...
                //no abort, simply re-try.
            }
            orderLock.advance();
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

    /**
     * This function shall be called for every input event *in order*
     *
     * @param t_record
     * @param txn_context
     * @param accessType
     */
    private void InsertLock(TableRecord t_record, TxnContext txn_context, CommonMetaTypes.AccessType accessType) {
        switch (accessType) {
            case READ_ONLY:
//				while (!t_record.content_.AcquireReadLock()) {
//					txn_context.is_retry_ = true;//retry, no abort..
//				}
                boolean rt = t_record.content_.AcquireReadLock();//it should always success;
                assert rt;
                break;
            case READ_WRITE:
                while (!t_record.content_.AcquireWriteLock() && !Thread.currentThread().isInterrupted()) {//Could it be two transactions concurrently writing the d_record? NO. it's protected by order lock_ratio.
                    txn_context.is_retry_ = true;//retry, no abort..
                }
                t_record.content_.AddLWM(txn_context.getBID());
                break;
            default:
        }
    }

    //The following makes sure the lock_ratio is added in event sequence as in ACEP.
    @Override
    protected boolean lock_aheadCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        InsertLock(t_record, txn_context, accessType);
        return true;
    }

    @Override
    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        if (accessType == READ_ONLY) {
            SchemaRecord local_record = t_record.content_.ReadAccess(txn_context, accessType);// return the correct version.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
            access.table_id_ = table_name;
            access.local_record_ = local_record;
            record_ref.setRecord(local_record);
            return true;
        } else if (accessType == READ_WRITE) {
            SchemaRecord local_record = t_record.content_.ReadAccess(txn_context, accessType);// return the correct version.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
            access.local_record_ = local_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            record_ref.setRecord(local_record);//the application can only access to a local copy at this point of time.
            return true;
        } else {//does not support deletion..
            assert (false);
            return false;
        }
    }

    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        //Different from locking scheme, LWM returns only local copy... The actual install happens later at commit stage.
        if (accessType == READ_ONLY) {
            //The following makes sure the lock_ratio is added in event sequence
            InsertLock(t_record, txn_context, accessType);
            SchemaRecord local_record = t_record.content_.ReadAccess(txn_context, accessType);// return the correct version.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
            access.table_id_ = table_name;
            access.local_record_ = local_record;
            record_ref.setRecord(local_record);
            return true;
        } else if (accessType == READ_WRITE) {
            InsertLock(t_record, txn_context, accessType);
            SchemaRecord local_record = t_record.content_.ReadAccess(txn_context, accessType);// return the correct version.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
            access.local_record_ = local_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            record_ref.setRecord(local_record);//the application can only access to a local copy at this point of time.
            return true;
        } else {//does not support deletion..
            assert (false);
            return false;
        }
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {
        boolean is_success = true;
        long certify_count = 0;// count number of certify locks.

		/*
			-- This is not required, as lwm will strictly guarantee the read & write sequence.
		// upgrade write lock_ratio to certify lock_ratio.
		for (int i = 0; i < access_list_.access_count_; ++i) {
			Access access_ptr = access_list_.GetAccess(i);
			if (access_ptr.access_type_ == READ_WRITE) {
				// try to upgrade to certify lock_ratio.
				if (!access_ptr.access_record_.content_.AcquireCertifyLock()) {
					is_success = false;
					break;
				} else {
					++certify_count;
				}
			}
		}*/
        // install.
        long commit_timestamp = txn_context.getBID();
        if (is_success) {
//			long curr_epoch = Epoch.GetEpoch();
//			commit_timestamp = GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp.GetMonotoneTimestamp());
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                if (access_ptr.access_type_ == READ_WRITE) {
                    // install from local copy.
                    access_ptr.access_record_.content_.WriteAccess(commit_timestamp, commit_timestamp, true, access_ptr.local_record_);
                }
            }
        }
        // release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            Access access_ptr = access_list_.GetAccess(i);
            if (access_ptr.access_type_ == READ_ONLY) {
                access_ptr.access_record_.content_.ReleaseReadLock();
            } else if (access_ptr.access_type_ == READ_WRITE) {
//				if (certify_count > 0) {
//					access_ptr.access_record_.content_.ReleaseCertifyLock();
//					--certify_count;
//				} else {
                access_ptr.access_record_.content_.ReleaseWriteLock();
//				}
            }
        }
        assert (certify_count == 0);
        if (is_success) {
            // clean up.
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                access_ptr.access_record_.content_.DeleteLWM(txn_context.getBID());
                if (access_ptr.access_type_ == READ_ONLY || access_ptr.access_type_ == READ_WRITE) {
                    access_ptr.local_record_ = null;
                }
            }
//			GlobalTimestamp::SetThreadTimestamp (thread_id_, commit_timestamp);
        } else {
            for (int i = 0; i < access_list_.access_count_; ++i) {
                Access access_ptr = access_list_.GetAccess(i);
                access_ptr.local_record_ = null;
            }
        }
        assert (access_list_.access_count_ <= kMaxAccessNum);
        access_list_.Clear();
        is_first_access_ = true;
//		END_PHASE_MEASURE(thread_id_, COMMIT_PHASE);
        return is_success;
    }

    @Override
    public void AbortTransaction() {
        //not in use in this scheme.
    }
}
