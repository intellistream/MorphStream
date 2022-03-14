package scheduler.struct.og.nonstructured;

import content.common.CommonMetaTypes;
import scheduler.context.og.AbstractOGNSContext;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class NSAOperation extends NSOperation {
    public final AbstractOGNSContext context;
    public int txnOpId = 0;
    // logical dependencies are to be stored for the purpose of abort handling
    private NSAOperation ld_head_operation = null; // the logical dependencies ops to be executed after this op.
    private final Queue<NSAOperation> ld_descendant_operations;
    private NSAOperationChain oc; // used for dependency resolved notification under greedy smart

    public NSAOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public NSAOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        Function function, Condition condition, int[] success) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public NSAOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends AbstractOGNSContext> NSAOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends AbstractOGNSContext> NSAOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends AbstractOGNSContext> NSAOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                              SchemaRecordRef record_ref) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }
    public <Context extends AbstractOGNSContext> NSAOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                              CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                              SchemaRecordRef record_ref,Function function) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, record_ref, function, null, null, null);
    }

    public <Context extends AbstractOGNSContext> NSAOperation(
            String pKey, Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(pKey, context, table_name, txn_context, bid, accessType, record, record_ref, function, condition, condition_records, success);
        this.context = context;
        ld_descendant_operations = new ArrayDeque<>();
    }

    public NSAOperationChain getOC() {
        return oc;
    }

    public void setOC(NSAOperationChain operationChain) {
        this.oc = operationChain;
    }

    public int getTxnOpId() {
        return txnOpId;
    }

    public void setTxnOpId(int txnOpId) {
        this.txnOpId = txnOpId;
    }

    /*********************************Dependencies setup****************************************/

    public void addHeader(NSAOperation header) {
        ld_head_operation = header;
    }

    public void addDescendant(NSAOperation descendant) {
//        oc.addDescendant(this, descendant);
        ld_descendant_operations.add(descendant);
    }

    public Collection<NSAOperation> getDescendants() {
        return ld_descendant_operations;
    }

    public NSAOperation getHeader() {
        return ld_head_operation;
    }
}