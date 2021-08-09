package transaction.scheduler.tpg.struct;

import common.meta.CommonMetaTypes;
import storage.SchemaRecordRef;
import storage.TableRecord;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnContext;
import transaction.scheduler.tpg.TPGContext;

import java.util.List;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class Operation extends OperationStateMachine implements Comparable<Operation> {
    public final TableRecord d_record;
    public final CommonMetaTypes.AccessType accessType;
    public final TxnContext txn_context;
    public final long bid;
    //required by READ_WRITE_and Condition.
    public final Function function;
    public final String table_name;
    public final TPGContext context;
    private final String operationChainKey;
    // operation id under a transaction.
    // an operation id to indicate how many operations in front of this operation in the same transacion.
    public int txn_op_id = 0;
    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile SchemaRecordRef record_ref;//required by read-only: the place holder of the reading d_record.
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
    public Condition condition;
    public int[] success;
    public boolean isFailed;
    public String name;

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, null, null, null);
    }

    /****************************Defined by MYC*************************************/

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, null, function, condition, success);
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        this(null, table_name, txn_context, bid, accessType, record, record_ref, function, condition, success);
    }


    public <Context extends TPGContext> Operation(Context context, String table_name, TxnContext txn_context, long bid,
                                                  CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, Condition condition, int[] success) {
        this(context, table_name, txn_context, bid, accessType, d_record, null, function, condition, success);
    }

    public <Context extends TPGContext> Operation(Context context, String table_name, TxnContext txn_context, long bid,
                                                  CommonMetaTypes.AccessType accessType, TableRecord d_record,
                                                  SchemaRecordRef record_ref) {
        this(context, table_name, txn_context, bid, accessType, d_record, record_ref, null, null, null);
    }

    public <Context extends TPGContext> Operation(
            Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        super();
        this.context = context;
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.function = function;
        this.s_record = d_record;
        this.record_ref = record_ref;//this holds events' record_ref.
        this.condition = condition;
        this.success = success;
        this.operationChainKey = table_name + "|" + d_record.record_.GetPrimaryKey();

    }


    /****************************End*************************************/


    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    @Override
    public int compareTo(Operation operation) {
        if (this.bid == (operation.bid)) {
            if (this.d_record.getID() - operation.d_record.getID() == 0) {
                return this.getTxn_op_id() - operation.getTxn_op_id();
            }
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    @Override
    public String toString() {
        return bid + "|" + txn_op_id + "|" + String.format("%-15s", this.getOperationState());
    }

    public void set_op_id(int op_id) {
        this.txn_op_id = op_id;
    }

    public int getTxn_op_id() {
        return txn_op_id;
    }

    /*********************************CREATED BY MYC****************************************/


    public void setExecutableOperationListener(TaskPrecedenceGraph.ShortCutListener shortCutListener) {
    }

    public String getOperationChainKey() {
        return operationChainKey;
    }
}