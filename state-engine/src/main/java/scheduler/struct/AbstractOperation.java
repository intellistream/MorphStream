package scheduler.struct;

import content.common.CommonMetaTypes;
import scheduler.struct.bfs.BFSOperation;
import storage.SchemaRecordRef;
import storage.TableRecord;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.List;

/**
 * TODO: clean ``state" and ``reference".
 */
public abstract class AbstractOperation {

    //required by READ_WRITE_and Condition.
    public final Function function;
    public final String table_name;
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final TableRecord d_record;
    public final long bid;
    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile SchemaRecordRef record_ref;//required by read-only: the placeholder of the reading d_record.
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
    public Condition condition;
    public int[] success;
    // an operation id to indicate how many operations in front of this operation in the same transaction.
    public boolean aborted;

    public AbstractOperation(Function function, String table_name, SchemaRecordRef record_ref, TableRecord[] condition_records, Condition condition, int[] success,
                             TxnContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, long bid) {
        this.function = function;
        this.table_name = table_name;
        this.record_ref = record_ref;//this holds events' record_ref.
        this.condition_records = condition_records;
        this.condition = condition;
        this.success = success;
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.s_record = s_record;
        this.d_record = d_record;
        this.bid = bid;
    }

    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    public int compareTo(BFSOperation operation) {
        if (this.bid == (operation.bid)) {
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    @Override
    public String toString() {
        return String.valueOf(bid);
    }
}
