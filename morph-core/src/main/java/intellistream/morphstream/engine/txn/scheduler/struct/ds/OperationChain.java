package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;
import lombok.Getter;
import lombok.Setter;

public class OperationChain implements Comparable<OperationChain> {
    @Setter @Getter
    private DSContext dsContext;
    public final MyList<Operation> operations;
    @Getter
    private final String tableName;
    @Getter
    private final String primaryKey;
    @Getter
    private Object tempValue;
    @Setter @Getter
    private boolean isLocalState = false;
    public int tryTimes = 0;
    private int refNumber = 0;
    public OperationChain(String tableName, String primaryKey) {
        this.primaryKey = primaryKey;
        this.tableName = tableName;
        operations = new MyList<>(tableName, primaryKey);
    }
    public void addOperation(Operation operation, boolean isRef) {
        boolean isadd = operations.add(operation);
        if (isRef) {
            refNumber ++;
        }
        if (!isadd) {
            System.out.println("add operation failed");
        }
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
                prevOperation = curOperation;
            }
        }
    }
    public boolean isFinished() {
        return operations.isEmpty();
    }

    @Override
    public int compareTo(OperationChain o) {
        return this.primaryKey.compareTo(o.primaryKey);
    }
    public void setTempValue(Object tempValue) {
        if (tempValue == null) {
            throw new NullPointerException("tempValue is null");
        }
        this.tempValue = tempValue;
    }
}
