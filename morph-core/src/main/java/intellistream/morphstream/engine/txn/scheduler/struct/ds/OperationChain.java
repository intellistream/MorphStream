package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        List<Operation> buffer = new ArrayList<>();
        List<Operation> localBuffer = new ArrayList<>();
        List<Operation> toRemove = new ArrayList<>();
        Operation tempOperation = null;
        for (Operation operation : operations) {
            tempOperation = operation;
            if (tempOperation.accessType == CommonMetaTypes.AccessType.READ && tempOperation.isReference) {
                buffer.add(tempOperation);
            } else if (tempOperation.accessType == CommonMetaTypes.AccessType.READ) {
                localBuffer.add(tempOperation);
            } else if (tempOperation.accessType == CommonMetaTypes.AccessType.WRITE) {
                if (!buffer.isEmpty()) {
                    aggregateRead(buffer, localBuffer, toRemove, tempOperation);
                    buffer.clear();
                }
                if (!localBuffer.isEmpty()) {
                    localBuffer.clear();
                }
            }
        }
        if (!buffer.isEmpty()) {
            aggregateRead(buffer, localBuffer, toRemove, tempOperation);
        }
        toRemove.forEach(operations::remove);
        dsContext.scheduledOperations += toRemove.size();
//        Operation prevOperation = null;
//        for (Operation curOperation : operations) {
//            if (prevOperation == null) {
//                prevOperation = curOperation;
//            } else {
//                curOperation.updateDependencies(prevOperation.brothers);
//                prevOperation = curOperation;
//            }
//        }
    }
    private void aggregateRead(List<Operation> buffer, List<Operation> localBuffer, List<Operation> toRemove, Operation write) {
        buffer.get(0).numberToRead = buffer.size();
        buffer.get(0).biggestBid = write.bid;
        buffer.get(0).localReads.addAll(localBuffer);
        for (int i = 1; i < buffer.size(); i++) {
            toRemove.add(buffer.get(i));
        }
        toRemove.addAll(localBuffer);
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
