package transaction.impl;

import content.common.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stage.Stage;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.context.TxnContext;

/**
 * No Locks at all.
 */
public class TxnManagerNoLock extends TxnManagerLock {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerNoLock.class);

    public TxnManagerNoLock(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count, Stage stage) {
        super(storageManager, thisComponentId, thisTaskId, thread_count, stage);
    }

    @Override
    public void AbortTransaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord
            t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_); //return the table record for modifying in the application layer.
        return true;
    }

    @Override
    public boolean CommitTransaction(TxnContext txnContext) {
        return true;
    }
}
