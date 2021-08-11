package transaction.scheduler.layered.struct;

import common.meta.CommonMetaTypes;
import storage.SchemaRecordRef;
import storage.TableRecord;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnContext;
import transaction.scheduler.common.AbstractOperation;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

//contains the place-holder to fill, as well as timestamp (counter).
public class Operation extends AbstractOperation implements Comparable<Operation> {
    //required by READ_WRITE_and Condition.
    private final Queue<Operation> dependents = new ConcurrentLinkedQueue<>();
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    public int column_id;
    //required by READ_WRITE.
    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public Condition condition;
    public String name;
    public boolean aborted = false;

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref, Function function) {
        this(table_name, record, record, record_ref, bid, accessType, function, null, null, txn_context, null);
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef record_ref) {
        this(table_name, record, record, record_ref, bid, accessType, null, null, null, txn_context, null);
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, TableRecordRef records_ref) {
        this(table_name, record, record, null, bid, accessType, null, null, null, txn_context, null);
        this.records_ref = records_ref;//this holds events' record_ref.
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, List<DataBox> value_list) {
        this(table_name, record, record, null, bid, accessType, null, null, null, txn_context, null);
        this.value_list = value_list;
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, long value, int column_id) {
        this(table_name, record, record, null, bid, accessType, null, null, null, txn_context, null);
        this.value = value;
        this.column_id = column_id;
    }

    /**
     * Update dest d_record by applying function of s_record.. It relys on MVCC to guarantee correctness.
     *
     * @param table_name
     * @param s_record
     * @param d_record
     * @param bid
     * @param accessType
     * @param function
     * @param txn_context
     * @param column_id
     */
    public Operation(String table_name, TableRecord s_record, TableRecord d_record, long bid, CommonMetaTypes.AccessType accessType, Function function, TxnContext txn_context, int column_id) {
        this(table_name, s_record, d_record, null, bid, accessType, function, null, null, txn_context, null);
        this.column_id = column_id;
    }

    public Operation(String table_name, TableRecord d_record, long bid, CommonMetaTypes.AccessType accessType,
                     Function function, TableRecord[] condition_records, Condition condition, TxnContext txn_context, int[] success) {
        this(table_name, d_record, d_record, null, bid, accessType, function, condition_records, condition, txn_context, success);
    }

    /**
     * @param table_name
     * @param s_record
     * @param d_record
     * @param record_ref
     * @param bid
     * @param accessType
     * @param function
     * @param condition_records
     * @param condition
     * @param txn_context
     * @param success
     */
    public Operation(String table_name, TableRecord s_record, TableRecord d_record, SchemaRecordRef record_ref, long bid,
                     CommonMetaTypes.AccessType accessType, Function function, TableRecord[] condition_records,
                     Condition condition, TxnContext txn_context, int[] success) {
        super(function, table_name, record_ref, condition_records, condition, success, txn_context, accessType, s_record, d_record, bid);
    }

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
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    public void setOc() {
    }

    public void addDependent(Operation dependent) {
        this.dependents.add(dependent);
    }

}