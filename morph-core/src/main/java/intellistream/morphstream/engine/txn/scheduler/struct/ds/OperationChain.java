package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;
import lombok.Getter;
import lombok.Setter;

public class OperationChain implements Comparable<OperationChain> {
    @Setter
    private DSContext dsContext;
    public final MyList<Operation> operations;
    @Getter
    private final String tableName;
    @Getter
    private final String primaryKey;
    public OperationChain(String tableName, String primaryKey) {
        this.primaryKey = primaryKey;
        this.tableName = tableName;
        operations = new MyList<>(tableName, primaryKey);
    }
    public void addOperation(Operation operation) {
        operations.add(operation);
    }
    public void deleteOperation(Operation operation) {
        operations.remove(operation);
        dsContext.scheduledOperations ++;
    }
    public void updateDependencies() {
        Operation prevOperation = null;
        for (Operation curOperation : operations) {
            if (prevOperation == null) {
                prevOperation = curOperation;
            } else {
                curOperation.updateDependencies(prevOperation.brothers);
            }
        }
    }
    public boolean isFinished() {
        return operations.isEmpty();
    }

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }
}
