package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.scheduler.struct.AbstractOperation;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class Operation extends AbstractOperation implements Comparable<Operation> {
    public final String pKey;
    public int addTimes = 0;
    public List<Operation> brothers = new ArrayList<>();
    public List<Operation> children = new ArrayList<>();
    public AtomicInteger fatherCount = new AtomicInteger(0);//We only need to count the number of fathers (lds) and invokes (pds), tds count can be ensured by skipList
    public int txnOpId = 0;
    public MetaTypes.OperationStateType operationType = MetaTypes.OperationStateType.BLOCKED;

    public Operation(String tableName, String pKey, long bid, boolean isRemote) {
        super(tableName, null, null, null, null, null, bid, null, pKey);
        this.pKey = pKey;
        this.isRemote = isRemote;
    }

    public <Context extends DSContext> Operation(String pKey, Context context, String table_name, FunctionContext txn_context, long bid,
                                                 CommonMetaTypes.AccessType accessType, TableRecord record,
                                                 HashMap<String, TableRecord> read_records, StateAccess stateAccess) {
        super(table_name, stateAccess, read_records, txn_context, accessType, record, bid, null, pKey);
        this.pKey = pKey;
        this.isRemote = false;
    }
    public void addBrother(Operation brother) {
        this.brothers.add(brother);
    }
    public void addToFather(Operation father) {
        father.addChild(this);
        this.fatherCount.incrementAndGet();
    }
    public void addChild(Operation child) {
        this.children.add(child);
    }
    public void updateDependencies(List<Operation> ldFather) {
        if (ldFather != null) {
            for (Operation father : ldFather) {
                this.addToFather(father);
            }
        }
    }
    public boolean isReady() {
        if (this.fatherCount.get() == 0) {
            this.operationType = MetaTypes.OperationStateType.READY;
            return true;
        } else {
            return false;
        }
    }
    public void tryToCommit(OperationChain oc) {
        int allExecuted = 0;
        for (Operation brother : brothers) {
            if (brother.operationType == MetaTypes.OperationStateType.ABORTED) {
                this.operationType = MetaTypes.OperationStateType.ABORTED;
                oc.deleteOperation(this);//TODO: add rollback state
                notifyChildren();
                return;
            } else if (brother.operationType == MetaTypes.OperationStateType.EXECUTED || brother.operationType == MetaTypes.OperationStateType.COMMITTED) {
                allExecuted ++;
            }
        }
        if (allExecuted == brothers.size()) {
            this.operationType = MetaTypes.OperationStateType.COMMITTED;
            notifyChildren();
            oc.deleteOperation(this);
        }
    }
    public boolean earlyAbort() {
        for (Operation brother : brothers) {
            if (brother.operationType == MetaTypes.OperationStateType.ABORTED) {
                this.operationType = MetaTypes.OperationStateType.ABORTED;
                notifyChildren();
                return true;
            }
        }
        return false;
    }
    public void notifyChildren() {
        for (Operation child : children) {
            child.fatherCount.getAndDecrement();
        }
    }
    @Override
    public int compareTo(Operation operation) {
//        if (this.bid == (operation.bid)) {
//            if (this.d_record.getID() - operation.d_record.getID() == 0) {
//                return this.getTxnOpId() - operation.getTxnOpId();
//            }
//            return this.d_record.getID() - operation.d_record.getID();
//        } else
            return Long.compare(this.bid, operation.bid);
    }
    public String getOperationRef() {
        return this.bid + ":" + this.table_name + ":" + this.pKey;
    }
}
