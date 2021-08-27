package scheduler.struct.gs;

import content.common.CommonMetaTypes;
import scheduler.context.AbstractGSTPGContext;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class GSOperationWithAbort extends GSOperation {
    public int txnOpId = 0;

    public final AbstractGSTPGContext context;
    // logical dependencies are to be stored for the purpose of abort handling
    private GSOperationWithAbort ld_head_operation = null; // the logical dependencies ops to be executed after this op.
    private GSOperationChainWithAbort oc; // used for dependency resolved notification under greedy smart

    public GSOperationWithAbort(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public GSOperationWithAbort(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                                Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public GSOperationWithAbort(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                                SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends AbstractGSTPGContext> GSOperationWithAbort(Context context, String table_name, TxnContext txn_context, long bid,
                                                                       CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends AbstractGSTPGContext> GSOperationWithAbort(Context context, String table_name, TxnContext txn_context, long bid,
                                                               CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends AbstractGSTPGContext> GSOperationWithAbort(Context context, String table_name, TxnContext txn_context, long bid,
                                                               CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                               SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends AbstractGSTPGContext> GSOperationWithAbort(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(context, table_name, txn_context, bid, accessType, record, record_ref, function, condition, condition_records, success);
        this.context = context;
    }

    public void setOC(GSOperationChainWithAbort operationChain) {
        this.oc = operationChain;
    }

    public GSOperationChainWithAbort getOC() {
        return oc;
    }

    public void setTxnOpId(int txnOpId) {
        this.txnOpId = txnOpId;
    }

    public int getTxnOpId() {
        return txnOpId;
    }

    /*********************************Dependencies setup****************************************/

    public void addHeader(GSOperationWithAbort header) {
        ld_head_operation = header;
    }

    public void addDescendant(GSOperationWithAbort descendant) {
        oc.addDescendant(this, descendant);
    }

    public GSOperationWithAbort getHeader() {
        return ld_head_operation;
    }
}