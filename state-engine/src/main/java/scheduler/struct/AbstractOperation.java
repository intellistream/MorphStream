package scheduler.struct;

import content.common.CommonMetaTypes;
import scheduler.oplevel.struct.MetaTypes.OperationStateType;
import storage.SchemaRecordRef;
import storage.TableRecord;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: clean ``state" and ``reference".
 */
public abstract class AbstractOperation implements Comparable<AbstractOperation>  {

    //required by READ_WRITE_and Condition.
    public final Function function;
    public final String table_name;
    public final String pKey;
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final TableRecord d_record;
    public final long bid;
    public volatile SchemaRecordRef record_ref;//required by read-only: the placeholder of the reading d_record.
    //only update corresponding column.
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
//    public String[] condition_sourceTable = null;
//    public String[] condition_source = null;
    public Condition condition;
    public int[] success;
    private final AtomicReference<OperationStateType> operationState;
    public boolean isFailed = false; // whether the operation is failed, this is used to detect transaction abort

//    public volatile AbstractOperation[] fdParentOps; // parent ops that accessing conditioned records and has smaller
    public volatile List<AbstractOperation> fd_parents; // parent ops that accessing conditioned records and has smaller
//    public HashMap<TableRecord, Integer> condition_source_to_index;

    public AbstractOperation(String pKey, Function function, String table_name, SchemaRecordRef record_ref, TableRecord[] condition_records, Condition condition, int[] success,
                             TxnContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, long bid) {
        this.pKey = pKey;
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
        if (condition_records != null) {
            this.fd_parents = new ArrayList<>();
        }
        operationState = new AtomicReference<>(OperationStateType.BLOCKED);
    }

    @Override
    public String toString() {
        return table_name + " " + d_record.record_.GetPrimaryKey() + " " + bid + " " + operationState.get();
    }

    /**
     * TODO: make it better.
     * It has an assumption that no duplicate keys for the same BID. --> This helps a lot!
     *
     * @param operation
     * @return
     */
    @Override
    public int compareTo(AbstractOperation operation) {
        if (this.bid == (operation.bid)) {
            if (!this.table_name.equals(operation.table_name)) {
                if (this.table_name.equals("accounts"))
                    return 1;
                else
                    return -1;
            }
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

//    public void setConditionSources(String[] condition_sourceTable, String[] condition_source) {
//        this.condition_sourceTable = condition_sourceTable;
//        this.condition_source = condition_source;
//    }


    public void addFDParent(AbstractOperation parent) {
//        assert condition_source_to_index.containsKey(pKey);
        fd_parents.add(parent);
    }

    public void stateTransition(OperationStateType state) {
        operationState.getAndSet(state);
    }

    public OperationStateType getOperationState() {
        return operationState.get();
    }
}
