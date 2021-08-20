package scheduler.struct;

import transaction.impl.ordered.MyList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
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

    private final MyList<Operation> operations;
    private final AtomicInteger oc_fd_parents_count;
    // OperationChainKey -> OperationChain
//    private final ConcurrentSkipListMap<String, OperationChain> oc_fd_parents; // functional dependent operation chains
    private final ConcurrentSkipListMap<OperationChain, Operation> oc_fd_parents;
//    private final AtomicInteger oc_fd_children_count; // TODO: should be deleted after code clean
    public boolean isExecuted = false;

//    private final ConcurrentHashMap<String, OperationChain> oc_fd_children; // functional dependent operation chains
    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operationChainKey = tableName + "|" + primaryKey;

        this.operations = new MyList<>(tableName, primaryKey);

        this.oc_fd_parents_count = new AtomicInteger(0);
//        this.oc_fd_children_count = new AtomicInteger(0);

        this.oc_fd_parents = new ConcurrentSkipListMap<>();
//        this.oc_fd_children = new ConcurrentHashMap<>();
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
    }

    public void addPotentialFDChildren(OperationChain potentialChildren, Operation op) {
        potentialChldrenInfo.add(new PotentialDependencyInfo(potentialChildren, op));
    }

    public void addFDParent(Operation targetOp, OperationChain parentOC) {
        Iterator<Operation> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            Operation parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
//                targetOp.addParent(parentOp, DependencyType.FD);
//                parentOp.addChild(targetOp, DependencyType.FD);
//                parentOC.addParentOrChild(this, DependencyType.FD, true);
//                this.addParentOrChild(parentOC, DependencyType.FD, false);
                this.oc_fd_parents.putIfAbsent(parentOC, parentOp);
                this.oc_fd_parents_count.incrementAndGet();
                break;
            }
        }
    }

    private String getOperationChainKey() {
        return operationChainKey;
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

//    public void addParentOrChild(OperationChain parentOrChildOC,
//                                  DependencyType dependencyType,
//                                  boolean addChild) {
//        ConcurrentHashMap<String, OperationChain> oc_relation;
//        AtomicInteger oc_relation_count;
//        // add dependent OCs found from op.
////        if (!addChild) {
//        if (dependencyType == DependencyType.FD) {
//            oc_relation = getOc_fd_parents();
//            oc_relation_count = getOc_fd_parents_count();
//        } else {
//            throw new IllegalStateException("Unexpected value: " + dependencyType);
//        }
////        }
////        else {
////            if (dependencyType == DependencyType.FD) {
//////                oc_relation = getOc_fd_children();
//////                oc_relation_count = getOc_fd_children_count();
////            } else {
////                throw new IllegalStateException("Unexpected value: " + dependencyType);
////            }
////        }
//        if (!parentOrChildOC.getOperationChainKey().equals(operationChainKey)) { // add to parent if not contained
//            OperationChain ret = oc_relation.putIfAbsent(parentOrChildOC.getOperationChainKey(), parentOrChildOC);
//            if (ret == null)
//                oc_relation_count.incrementAndGet();
//        }
//    }

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


    public AtomicInteger getOc_fd_parents_count() {
        return oc_fd_parents_count;
    }

    public boolean blocked() {
        return oc_fd_parents_count.get() > 0;
    }

//    public AtomicInteger getOc_fd_children_count() {
//        return oc_fd_children_count;
//    }

//    public ConcurrentHashMap<String, OperationChain> getOc_fd_parents() {
//        return oc_fd_parents;
//    }

//    public ConcurrentHashMap<String, OperationChain> getOc_fd_children() {
//        return oc_fd_children;
//    }

    public synchronized boolean hasValidDependencyLevel() {
        return isDependencyLevelCalculated;
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    public synchronized void updateDependencyLevel() {
        if (isDependencyLevelCalculated)
            return;
        dependencyLevel = 0;
        for (OperationChain oc : oc_fd_parents.keySet()) {
            if (!oc.hasValidDependencyLevel())
                oc.updateDependencyLevel();

            if (oc.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
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
