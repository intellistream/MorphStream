package intellistream.morphstream.engine.txn.scheduler.struct.og;

import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.struct.AbstractOperation;
import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.WindowDescriptor;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO: clean ``state" and ``reference".
 */
public class Operation extends AbstractOperation implements Comparable<Operation> {

    public final OGSchedulerContext context;
    public final Queue<Operation> fd_parents; // the functional dependencies ops to be executed in advance
    public final Queue<Operation> fd_children; // the functional dependencies ops to be executed after this op.
    protected final Queue<Operation> ld_descendant_operations;

//    public volatile Operation[] fdParentOps; // parent ops that accessing conditioned records and has smaller
//    public volatile List<Operation> fd_parents; // parent ops that accessing conditioned records and has smaller
//    public HashMap<TableRecord, Integer> condition_source_to_index;
    public AtomicBoolean isFailed = new AtomicBoolean(false); // whether the operation is failed, this is used to detect transaction abort
    public boolean isNonDeterministicOperation = false;
    public BaseTable[] tables;
    public TableRecord[] deterministicRecords;
    //required by READ_WRITE.
//    public String[] condition_sourceTable = null;
//    public String[] condition_source = null;
    private intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes.OperationStateType operationState;
    private int txnOpId = 0;
    // logical dependencies are to be stored for the purpose of abort handling
    private Operation ld_head_operation = null; // the logical dependencies ops to be executed after this op.
    private OperationChain oc; // used for dependency resolved notification under greedy smart

    public <Context extends OGSchedulerContext> Operation(Boolean isNonDeterministicOperation, BaseTable[] tables, String pKey, String table_name, HashMap<String, TableRecord> read_records,
                                                          FunctionContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord d_record, long bid, Context context, WindowDescriptor windowDescriptor, Function function) {
        super(table_name, function, read_records, txn_context, accessType, d_record, bid, windowDescriptor, pKey);

        // finctional dependencies, this should be concurrent because cross thread access
        fd_parents = new ConcurrentLinkedDeque<>(); // the finctional dependnecies ops to be executed in advance
        fd_children = new ConcurrentLinkedDeque<>();
        operationState = intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes.OperationStateType.BLOCKED;
        this.context = context;
        ld_descendant_operations = new ArrayDeque<>();
        this.isNonDeterministicOperation = isNonDeterministicOperation;
        this.tables = tables;
    }

    @Override
    public String toString() {
        return bid + "|" + txnOpId + "|" + String.format("%-15s", this.getOperationState());
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


    public void addFDParent(Operation parent) {
        fd_parents.add(parent);
    }

    public void addFDChild(Operation child) {
        fd_children.add(child);
    }

    public void stateTransition(intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes.OperationStateType state) {
        operationState = state;
    }

    public MetaTypes.OperationStateType getOperationState() {
        return operationState;
    }

    public int getTxnOpId() {
        return txnOpId;
    }

    public void setTxnOpId(int op_id) {
        this.txnOpId = op_id;
    }

    public OperationChain getOC() {
        return oc;
    }

    public void setOC(OperationChain operationChain) {
        this.oc = operationChain;
    }

    /*********************************Dependencies setup****************************************/

    public void addHeader(Operation header) {
        ld_head_operation = header;
    }

    public void addDescendant(Operation descendant) {
        ld_descendant_operations.add(descendant);
    }

    public Collection<Operation> getDescendants() {
        return ld_descendant_operations;
    }

    public Operation getHeader() {
        return ld_head_operation;
    }
}
