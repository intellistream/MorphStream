package transaction.scheduler.tpg.struct;

import transaction.dedicated.ordered.MyList;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    private final String tableName;
    private final String primaryKey;
    private final MyList<Operation> operations;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void addOperation(Operation op) {
        operations.add(op);
//        op.addOC(this);
        // TD need to be updated every time a new operation comes
        // TODO: or we can update when punctuation arrives.
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }


    public String getStringId() {
        if (tableName.contains("accounts")) {
            return String.format("act_%s", primaryKey);
        } else {
            return String.format("ast_%s", primaryKey);
        }
    }

    /**
     * Update TD of each operation in the operation chain
     *
     * OC: O1 <- O2 O3. O2
     *
     */
    public void updateTDDependencies() {
        Operation prevOperation = null;
        List<Operation> parentOperations = new ArrayList<>();
        for (Operation curOperation : operations) {
            if (prevOperation != null) {
                parentOperations.add(prevOperation);
                // if operations are in the same transaction, i.e. have the same bid,
                // add the temporal dependency parent of the prevOperation i.e. all operations with the same bid have the same temporal dependent parent
                if (curOperation.bid != prevOperation.bid) {
                    for (Operation parentOperation : parentOperations) {
                        curOperation.addParent(parentOperation, MetaTypes.DependencyType.TD);
                        parentOperation.addChild(curOperation, MetaTypes.DependencyType.TD);
                    }
                    parentOperations.clear();
                } else {
                    Queue<Operation> prevParentOperations = prevOperation.getParents(MetaTypes.DependencyType.TD);
                    for (Operation prevParentOperation : prevParentOperations) {
                        curOperation.addParent(prevParentOperation, MetaTypes.DependencyType.TD);
                        prevParentOperation.addChild(curOperation, MetaTypes.DependencyType.TD);
                    }
                }
            }
            prevOperation = curOperation;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationChain that = (OperationChain) o;
        return tableName.equals(that.tableName) &&
                primaryKey.equals(that.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, primaryKey);
    }

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }
}
