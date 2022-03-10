package scheduler.struct.og.structured.bfs;

import content.common.CommonMetaTypes;
import scheduler.context.og.OGSchedulerContext;
import scheduler.struct.og.AbstractOperation;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class BFSOperation extends AbstractOperation {

    public BFSOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public BFSOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        Function function, Condition condition, int[] success) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public BFSOperation(String pKey, String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(pKey, null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends OGSchedulerContext> BFSOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                             CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends OGSchedulerContext> BFSOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                             CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends OGSchedulerContext> BFSOperation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                             CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                             SchemaRecordRef record_ref) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends OGSchedulerContext> BFSOperation(
            String pKey, Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(pKey, function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid);
    }
}