package transaction.scheduler.tpg.struct;

import transaction.dedicated.ordered.MyList;
import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    private final String tableName;
    private final String primaryKey;
    private final String operationChainKey;

    private final ConcurrentLinkedQueue<PotentialDependencyInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();

    public TPGContext context = null;

    private final MyList<Operation> operations;

    public boolean isExecuted = false;

    private final AtomicInteger oc_fd_parents_count;
    private final AtomicInteger oc_ld_parents_count;
    private final AtomicInteger oc_fd_children_count; // TODO: in original implementation, we ignored that one OC might have multiple dependent op on the other OC.
    private final AtomicInteger oc_ld_children_count; // TODO: in original implementation, we ignored that one OC might have multiple dependent op on the other OC.

    // OperationChainKey -> OperationChain
    private final HashMap<String, OperationChain> oc_fd_parents; // functional dependent operation chains
    private final HashMap<String, OperationChain> oc_ld_parents; // logical dependent operation chains
    private final HashMap<String, OperationChain> oc_fd_children; // functional dependent operation chains
    private final HashMap<String, OperationChain> oc_ld_children; // logical dependent operation chains

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operationChainKey = tableName + "|" + primaryKey;

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

    private void setContext(TPGContext context) {
        if (this.context == null) {
            this.context = context;
        }
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
        setContext(op.context);
    }

    public void addPotentialChildren(OperationChain potentialChildren, Operation op) {
        potentialChldrenInfo.add(new PotentialDependencyInfo(potentialChildren, op));
    }

    public void addParent(Operation targetOp, OperationChain parentOC) {
        Iterator<Operation> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            Operation parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                targetOp.addParent(parentOp, DependencyType.FD);
                parentOp.addChild(targetOp, DependencyType.FD);

                this.addParentOrChild(targetOp, DependencyType.FD, false);
                parentOC.addParentOrChild(parentOp, DependencyType.FD, true);
                break;
            }
        }
    }

    private String getOperationChainKey() {
        return operationChainKey;
    }

    public void checkPotentialChildrenOnNewArrival(Operation newOp) {
        List<PotentialDependencyInfo> processed = new ArrayList<>();

        for (PotentialDependencyInfo pChildInfo : potentialChldrenInfo) {
            if (newOp.bid < pChildInfo.op.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pChildInfo.potentialChildOC.addParent(pChildInfo.op, this);
                processed.add(pChildInfo);
            }
        }
        potentialChldrenInfo.removeAll(processed);
        processed.clear();
    }

    /**
     *
     * @param op the operation to find the XX_parents
     * @param dependencyType XX dependency type
     * @param addChild whether is to add a child
     */
    private void addParentOrChild(Operation op,
                                  DependencyType dependencyType,
                                  boolean addChild) {
        if (!addChild) {
            HashMap<String, OperationChain> oc_relation;
            AtomicInteger oc_relation_count;
            switch (dependencyType) {
                case FD: {
                    oc_relation = getOc_fd_parents();
                    oc_relation_count = getOc_fd_parents_count();
                    break;
                }
                case LD: {
                    oc_relation = getOc_ld_parents();
                    oc_relation_count = getOc_ld_parents_count();
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + dependencyType);
            }
            // add dependent OCs found from op.
            Queue<Operation> parents = op.getParents(dependencyType);
            for (Operation parent : parents) {
                if (!parent.getOperationChainKey().equals(operationChainKey)) { // add to parent if not contained
                    OperationChain ret = oc_relation.putIfAbsent(parent.getOperationChainKey(), parent.getOC());
                    if (ret == null)
                        oc_relation_count.incrementAndGet();
                }
            }
        } else {
            HashMap<String, OperationChain> oc_relation;
            AtomicInteger oc_relation_count;
            switch (dependencyType) {
                case FD: {
                    oc_relation = getOc_fd_children();
                    oc_relation_count = getOc_fd_children_count();
                    break;
                }
                case LD: {
                    oc_relation = getOc_ld_parents();
                    oc_relation_count = getOc_ld_parents_count();
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + dependencyType);
            }
            // add dependent OCs found from op.
            Queue<Operation> children = op.getChildren(dependencyType);
            for (Operation child : children) {
                if (!child.getOperationChainKey().equals(operationChainKey)) { // add to parent if not contained
                    OperationChain ret = oc_relation.putIfAbsent(child.getOperationChainKey(), child.getOC());
                    if (ret == null)
                        oc_relation_count.incrementAndGet();
                }
            }
        }
    }

    public boolean hasChildren() {
        return getOc_fd_children_count().get() != 0;
    }
    public boolean hasParents() {
        return getOc_fd_parents_count().get() != 0;
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + "|" +  isExecuted + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    public String getStringId() {
        if (tableName.contains("accounts")) {
            return String.format("act_%s", primaryKey);
        } else {
            return String.format("ast_%s", primaryKey);
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
                getOc_fd_parents_count().decrementAndGet();
                break;
            }
            case LD: {
                getOc_ld_parents_count().decrementAndGet();
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dependencyType);
        }
    }

    public AtomicInteger getOc_fd_parents_count() {
        return oc_fd_parents_count;
    }

    public AtomicInteger getOc_ld_parents_count() {
        return oc_ld_parents_count;
    }

    public AtomicInteger getOc_fd_children_count() {
        return oc_fd_children_count;
    }

    public AtomicInteger getOc_ld_children_count() {
        return oc_ld_children_count;
    }

    public HashMap<String, OperationChain> getOc_fd_parents() {
        return oc_fd_parents;
    }

    public HashMap<String, OperationChain> getOc_ld_parents() {
        return oc_ld_parents;
    }

    public HashMap<String, OperationChain> getOc_fd_children() {
        return oc_fd_children;
    }

    public HashMap<String, OperationChain> getOc_ld_children() {
        return oc_ld_children;
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
