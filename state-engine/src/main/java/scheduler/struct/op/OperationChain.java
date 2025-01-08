package scheduler.struct.op;

import scheduler.struct.op.MetaTypes.DependencyType;
import transaction.impl.ordered.MyList;
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies' construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    // layered OC related
    public final ConcurrentHashMap<OperationChain, Operation> ocParents; // for layered TPG building
    private final String tableName;
    private final String primaryKey;
    private final ConcurrentLinkedQueue<PotentialDependencyInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();
    private final MyList<Operation> operations;
    private final MyList<Operation> operationWithVirtual;//To identify the dependencies
    public boolean isExecuted = false;
    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
        this.ocParents = new ConcurrentHashMap<>();
        this.operationWithVirtual = new MyList<>(tableName, primaryKey);
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
    public void updateDependencies() {
        Operation prevOperation = null;
        List<Operation> parentOperations = new ArrayList<>();
        List<Operation> nonDeterministicOperations = new ArrayList<>();
        for (Operation curOperation : operationWithVirtual) {
            if (prevOperation != null) {
                if (curOperation.isNonDeterministicOperation) {
                    updateNonDependencies(curOperation, parentOperations, prevOperation);
                    nonDeterministicOperations.add(curOperation);
                } else if (!curOperation.pKey.equals(this.primaryKey)){
                    updateFDDependencies(curOperation, parentOperations, prevOperation);
                } else {
                    updateTDDependencies(curOperation, parentOperations, prevOperation, nonDeterministicOperations);
                    prevOperation = curOperation;
                }
            } else {
               if (curOperation.pKey.equals(this.primaryKey) || curOperation.isNonDeterministicOperation)
                   prevOperation = curOperation;
            }
        }
    }
    public void initializeDependencies() {
        for (Operation operation : operations) {
            operation.initialize();
        }
    }
    public void updateDependencies(Operation childOperation, Operation parentOperation, DependencyType dependencyType) {
        childOperation.addParent(parentOperation, dependencyType);
        parentOperation.addChild(childOperation, dependencyType);
    }
    public void updateTDDependencies(Operation childOperation, List<Operation> parentOperations, Operation prevOperation, List<Operation> nonDeterministicOperations) {
        // if operations are in the same transaction, i.e. have the same bid,
        // add the temporal dependency parent of the prevOperation i.e. all operations with the same bid have the same temporal dependent parent
        if (childOperation.bid != prevOperation.bid) {
            if (nonDeterministicOperations.size() > 0) {
                for (Operation nonDeterministicOperation : nonDeterministicOperations) {
                    updateDependencies(childOperation, nonDeterministicOperation, DependencyType.TD);
                }
                nonDeterministicOperations.clear();
            } else {
                parentOperations.add(prevOperation);
                for (Operation parentOperation : parentOperations) {
                    updateDependencies(childOperation, parentOperation, DependencyType.TD);
                }
                parentOperations.clear();
            }
        } else {
            parentOperations.add(prevOperation);
            Queue<Operation> prevParentOperations = prevOperation.getParents(DependencyType.TD);
            for (Operation prevParentOperation : prevParentOperations) {
                updateDependencies(childOperation, prevParentOperation, DependencyType.TD);
            }
        }
    }
    public void updateFDDependencies(Operation childOperation, List<Operation> parentOperations, Operation prevOperation) {
        parentOperations.add(prevOperation);
        for (Operation parentOperation : parentOperations) {
            updateDependencies(childOperation, parentOperation, DependencyType.FD);
        }
        parentOperations.clear();
    }
    public void updateNonDependencies(Operation childOperation, List<Operation> parentOperations, Operation prevOperation) {
        if (prevOperation != null) {
            parentOperations.add(prevOperation);
            if (childOperation.bid != prevOperation.bid) {
                for (Operation parentOperation : parentOperations) {
                    updateDependencies(childOperation, parentOperation, DependencyType.FD);
                }
                parentOperations.clear();
            }
        }
    }

    public void addOperation(Operation op) {
        operations.add(op);
        operationWithVirtual.add(op);
    }

    public void addPotentialFDChildren(OperationChain potentialChildren, Operation op) {
        operationWithVirtual.add(op);
    }
    public void addNonOperation(ConcurrentSkipListSet<Operation> nonOperations) {
        operationWithVirtual.addAll(nonOperations);
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + "|" + isExecuted + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
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

    public void clear() {
        potentialChldrenInfo.clear();
        if (operations.size() != 0) {
//            operations.first().d_record.content_.clean_map(); //Disabled GC for ED
            operations.clear();
            operationWithVirtual.clear();
        }
        ocParents.clear();
        isExecuted = false;
        // Structured data structure clearance
        isDependencyLevelCalculated = false;
        dependencyLevel = -1;
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
            return Double.compare(this.op.bid, o.op.bid);
        }
    }
}
