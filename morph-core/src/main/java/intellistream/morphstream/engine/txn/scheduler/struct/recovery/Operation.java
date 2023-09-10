package intellistream.morphstream.engine.txn.scheduler.struct.recovery;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.context.recovery.RSContext;
import intellistream.morphstream.engine.txn.scheduler.struct.AbstractOperation;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Function;

import java.util.concurrent.atomic.AtomicInteger;

public class Operation extends AbstractOperation implements Comparable<Operation> {
    public final RSContext context;
    public final String pKey;
    public Object historyView = null;
    public AtomicInteger pdCount = new AtomicInteger(0);// We only ensure pdCount, TD count can be ensured by skipList
    public int txnOpId = 0;
    public OperationChain dependentOC;
    public boolean isFailed = false;
    public MetaTypes.OperationStateType operationState = MetaTypes.OperationStateType.BLOCKED;

    public <Context extends RSContext> Operation(String pKey, Context context, String table_name, TxnContext txn_context, long bid,
                                                 CommonMetaTypes.AccessType accessType, TableRecord d_record, Function function, TableRecord[] condition_records, int[] success) {
        this(pKey, context, table_name, txn_context, bid, accessType, d_record, null, function, condition_records, success);
    }

    public <Context extends RSContext> Operation(
            String pKey, Context context, String table_name, TxnContext txn_context, long bid,
            CommonMetaTypes.AccessType accessType, TableRecord record,
            SchemaRecordRef record_ref, Function function,
            TableRecord[] condition_records, int[] success) {
        super(function, table_name, record_ref, condition_records, success, txn_context, accessType, record, bid, null, pKey);
        this.context = context;
        this.pKey = pKey;
    }

    @Override
    public int compareTo(Operation operation) {
        if (this.bid == (operation.bid)) {
            if (this.d_record.getID() - operation.d_record.getID() == 0) {
                return this.getTxnOpId() - operation.getTxnOpId();
            }
            return this.d_record.getID() - operation.d_record.getID();
        } else
            return Long.compare(this.bid, operation.bid);
    }

    public int getTxnOpId() {
        return txnOpId;
    }

    public void setTxnOpId(int op_id) {
        this.txnOpId = op_id;
    }

    public void incrementPd(OperationChain oc) {
        this.pdCount.getAndIncrement();
        this.dependentOC = oc;
    }
}
