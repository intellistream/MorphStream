package scheduler.oplevel.struct;

import scheduler.oplevel.context.OPGSTPGContext;
import scheduler.oplevel.struct.MetaTypes.DependencyType;
import transaction.impl.ordered.MyList;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    private final String tableName;
    private final String primaryKey;

    private final ConcurrentLinkedQueue<PotentialDependencyInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();

    private final MyList<Operation> operations;
    public boolean isExecuted = false;

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

    /**
     * Update TD of each operation in the operation chain
     * <p>
     * OC: O1 <- O2 O3. O2
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
                        curOperation.addParent(parentOperation, DependencyType.TD);
                        parentOperation.addChild(curOperation, DependencyType.TD);
                    }
                    parentOperations.clear();
                } else {
                    Queue<Operation> prevParentOperations = prevOperation.getParents(DependencyType.TD);
                    for (Operation prevParentOperation : prevParentOperations) {
                        curOperation.addParent(prevParentOperation, DependencyType.TD);
                        prevParentOperation.addChild(curOperation, DependencyType.TD);
                    }
                }
            }
            prevOperation = curOperation;
        }
    }

    public void addOperation(Operation op) {
        operations.add(op);
    }

    public void addPotentialFDChildren(OperationChain potentialChildren, Operation op) {
        potentialChldrenInfo.add(new PotentialDependencyInfo(potentialChildren, op));
    }

    public void addFDParent(Operation targetOp, OperationChain parentOC) {
        Iterator<Operation> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            Operation parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                targetOp.addParent(parentOp, DependencyType.FD);
                parentOp.addChild(targetOp, DependencyType.FD);
                break;
            }
        }
    }

    public void checkPotentialFDChildrenOnNewArrival(Operation newOp) {
        List<PotentialDependencyInfo> processed = new ArrayList<>();

        for (PotentialDependencyInfo pChildInfo : potentialChldrenInfo) {
            if (newOp.bid < pChildInfo.op.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pChildInfo.potentialChildOC.addFDParent(pChildInfo.op, this);
                processed.add(pChildInfo);
            }
        }
        potentialChldrenInfo.removeAll(processed);
        processed.clear();
    }


    public MyList<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + "|" +  isExecuted + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
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

    public class PotentialDependencyInfo implements Comparable<PotentialDependencyInfo> {
        public OperationChain potentialChildOC;
        public Operation op;

        public PotentialDependencyInfo(OperationChain oc, Operation op) {
            this.potentialChildOC = oc;
            this.op = op;
        }

        @Override
        public int compareTo(PotentialDependencyInfo o) {
            return Long.compare(this.op.bid, o.op.bid);
        }
    }
}
