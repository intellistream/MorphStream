package scheduler.struct.bfs;

import content.common.CommonMetaTypes;
import scheduler.context.SchedulerContext;
import scheduler.struct.AbstractOperation;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class BFSOperation extends AbstractOperation implements Comparable<BFSOperation> {
    public BFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public BFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, null, success);
    }

    public BFSOperation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                        SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, null, success);
    }


    public <Context extends SchedulerContext> BFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, TableRecord[] condition_records, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, condition_records, success);
    }

    public <Context extends SchedulerContext> BFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, null, null, null, null);
    }

    public <Context extends SchedulerContext> BFSOperation(Context context, String table_name, TxnContext txn_context, long bid,
                                                           CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                           SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null, null);
    }

    public <Context extends SchedulerContext> BFSOperation(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition,
            TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, record, record, bid);
    }

    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    @Override
    public int compareTo(BFSOperation operation) {
        if (this.bid == (operation.bid)) {
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }
}