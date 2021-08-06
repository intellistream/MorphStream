package transaction.scheduler.tpg.struct;

import common.meta.CommonMetaTypes;
import storage.SchemaRecordRef;
import storage.TableRecord;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.function.Condition;
import transaction.function.Function;
import transaction.impl.TxnContext;


import java.util.List;
import java.util.Queue;

/**
 * contains the place-holder to fill, as well as timestamp (counter).
 */
public class Operation extends OperationStateMachine implements Comparable<Operation> {
    public final TableRecord d_record;
    public final CommonMetaTypes.AccessType accessType;
    public final TxnContext txn_context;
    public final long bid;

    // operation id under a transaction.
    // an operation id to indicate how many operations in front of this operation in the same transacion.
    public int txn_op_id = 0;
    public int txn_id = 0;

    //required by READ_WRITE_and Condition.
    public final Function function;
    public final String table_name;

    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile SchemaRecordRef record_ref;//required by read-only: the place holder of the reading d_record.
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    public int column_id;
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
    public Condition condition;
    public int[] success;
    public String name;

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record, SchemaRecordRef recordRef) {
        super();
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.s_record = d_record;
        this.function = null;
        this.record_ref = recordRef;//this holds events' recordRef.
    }


    /****************************Defined by MYC*************************************/

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     SchemaRecordRef record_ref, Function function, Condition condition, int[] success) {
        super();
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
    }

    public Operation(String table_name, TxnContext txn_context, long bid, CommonMetaTypes.AccessType accessType, TableRecord record,
                     Function function, Condition condition, int[] success) {
        super();
        this.table_name = table_name;
        this.d_record = record;
        this.bid = bid;
        this.accessType = accessType;
        this.txn_context = txn_context;
        this.function = function;
        this.s_record = d_record;
        this.record_ref = null;
        this.condition = condition;
        this.success = success;
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

    public void set_worker(String name) {
        assert this.name == null;
        this.name = name;
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

    @Override
    protected boolean checkOthertxnOperation(Queue<OperationStateMachine> td_parents, MetaTypes.OperationStateType targetState) {
        boolean isInTargetState = true;
        for (OperationStateMachine operation : td_parents) {
            if (((Operation) operation).bid != this.bid) {
                // if not belong to the same transaction, then check whether committed
                if (!operation.getOperationState().equals(targetState)) {
                    isInTargetState = false;
                }
            }
        }
        return isInTargetState;
    }
}