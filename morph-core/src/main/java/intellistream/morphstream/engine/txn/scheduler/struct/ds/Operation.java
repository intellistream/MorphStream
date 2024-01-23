package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.AbstractOperation;
import intellistream.morphstream.engine.txn.scheduler.struct.op.WindowDescriptor;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import lombok.Getter;

import java.util.HashMap;

@Getter
public class Operation extends AbstractOperation implements Comparable<Operation> {
    public int txnOpId = 0;

    public Operation(String table_name, StateAccess stateAccess, HashMap<String, TableRecord> condition_records, TxnContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord d_record, long bid, WindowDescriptor windowContext, String pKey) {
        super(table_name, stateAccess, condition_records, txn_context, accessType, d_record, bid, windowContext, pKey);
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
}
