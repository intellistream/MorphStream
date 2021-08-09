package transaction.dedicated;

import common.meta.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.impl.TxnContext;

/**
 * No Locks at all.
 */
public class TxnManagerNoLock extends TxnManagerLock {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerNoLock.class);

    public TxnManagerNoLock(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
    }

    @Override
    public void AbortTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord
            t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_); //return the table record for modifying in the application layer.
//        Access access = access_list_.NewAccess();
//        access.access_type_ = accessType;
//        access.access_record_ = t_record;
//        access.local_record_ = null;
//        access.table_id_ = table_name;
//        access.timestamp_ = t_record.content_.GetTimestamp();
        return true;
    }

    @Override
    public boolean CommitTransaction(TxnContext txnContext) {
//        for (int i = 0; i < access_list_.access_count_; ++i) {
//            Access access_ptr = access_list_.GetAccess(i);
//            if (access_ptr.access_type_ == READ_ONLY) {
//                access_ptr.access_record_.content_.ReleaseReadLock();
//            } else if (access_ptr.access_type_ == READ_WRITE) {
////                SchemaRecord local_record_ptr = access_ptr.local_record_;
//                access_ptr.access_record_.content_.ReleaseWriteLock();
////                local_record_ptr.clean();
//            } else {
//                // insert_only or delete_only
//                access_ptr.access_record_.content_.ReleaseWriteLock();
//            }
//        }
//        assert (access_list_.access_count_ <= kMaxAccessNum);
//        access_list_.Clear();
        return true;
    }
}
