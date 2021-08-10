package transaction.scheduler.tpg.struct;

import transaction.dedicated.ordered.MyList;
import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    private final String tableName;
    private final String primaryKey;
    private final String operationChainKey;

    public final TPGContext context;

    private final MyList<Operation> operations;

    private final AtomicInteger oc_fd_parents_count;
    private final AtomicInteger oc_ld_parents_count;
    private final AtomicInteger oc_fd_children_count; // TODO: in original implementation, we ignored that one OC might have multiple dependent op on the other OC.
    private final AtomicInteger oc_ld_children_count; // TODO: in original implementation, we ignored that one OC might have multiple dependent op on the other OC.

    // OperationChainKey -> OperationChain
    private final HashMap<String, OperationChain> oc_fd_parents; // functional dependent operation chains
    private final HashMap<String, OperationChain> oc_ld_parents; // logical dependent operation chains
    private final HashMap<String, OperationChain> oc_fd_children; // functional dependent operation chains
    private final HashMap<String, OperationChain> oc_ld_children; // logical dependent operation chains

    public OperationChain(String tableName, String primaryKey, TPGContext context) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operationChainKey = tableName + "|" + primaryKey;
        this.context = context;

        this.operations = new MyList<>(tableName, primaryKey);

        this.oc_fd_parents_count = new AtomicInteger(0);
        this.oc_ld_parents_count = new AtomicInteger(0);
        this.oc_fd_children_count = new AtomicInteger(0);
        this.oc_ld_children_count = new AtomicInteger(0);

        this.oc_fd_parents = new HashMap<>();
        this.oc_ld_parents = new HashMap<>();
        this.oc_fd_children = new HashMap<>();
        this.oc_ld_children = new HashMap<>();
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void addOperation(Operation op) {
        op.setOC(this);
        operations.add(op);
        addParentOrChild(op, oc_fd_parents, oc_fd_parents_count, DependencyType.FD);
        addParentOrChild(op, oc_ld_parents, oc_ld_parents_count, DependencyType.LD);
        addParentOrChild(op, oc_fd_children, oc_fd_children_count, DependencyType.FD);
        addParentOrChild(op, oc_ld_children, oc_ld_children_count, DependencyType.LD);
    }

    private void addParentOrChild(Operation op,
                                  HashMap<String, OperationChain> oc_relation,
                                  AtomicInteger oc_relation_count,
                                  DependencyType dependencyType) {
        // add dependent OCs found from op.
        Queue<Operation> parents = op.getParents(dependencyType);
        for (Operation parent : parents) {
            if (!parent.getOperationChainKey().equals(operationChainKey)) { // add to parent if not contained
                oc_relation.putIfAbsent(parent.getOperationChainKey(), parent.getOC());
                oc_relation_count.incrementAndGet();
            }
        }
    }

    public boolean hasChildren() {
        return oc_fd_children_count.get() == 0;
    }
    public boolean hasParents() {
        return oc_fd_parents_count.get() == 0;
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

    public void updateDependencies(DependencyType dependencyType) {
        switch (dependencyType) {
            case FD: {
                oc_fd_parents_count.decrementAndGet();
                break;
            }
            case LD: {
                oc_ld_parents_count.decrementAndGet();
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dependencyType);
        }
    }
}
