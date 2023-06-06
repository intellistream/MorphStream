package scheduler.struct.og;

import scheduler.context.og.OGSchedulerContext;
import transaction.impl.ordered.MyList;
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain implements Comparable<OperationChain> {
    public final String tableName;
    public final String primaryKey;
    public final double bid;
    public final AtomicInteger ocParentsCount;
    // OperationChain -> ChildOp that depend on the parent OC in cur OC
    public final ConcurrentHashMap<OperationChain, Operation> ocParents;
    public final ConcurrentHashMap<OperationChain, Operation> ocChildren;
    protected final MyList<Operation> operations;
    private final ConcurrentLinkedQueue<PotentialChildrenInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();
    public boolean isExecuted = false;

//    protected TaskPrecedenceGraph tpg;

    public OGSchedulerContext context = null;
    public boolean needAbortHandling = false; // The abort handling in GS should be residing in each operation chain
    public Queue<Operation> failedOperations = new ArrayDeque<>();

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;


//    private final HashSet<OperationChain> scanedOCs = new HashSet<>();

    public OperationChain(String tableName, String primaryKey, double bid) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.bid = bid;
        this.operations = new MyList<>(tableName, primaryKey);
        this.ocParentsCount = new AtomicInteger(0);
        this.ocParents = new ConcurrentHashMap<>();
        this.ocChildren = new ConcurrentHashMap<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void addOperation(Operation op) {
        operations.add(op);
        op.setOC(this); // set OC for op to enable txn abort.
    }

    public void addPotentialFDChildren(OperationChain potentialChildren, Operation op) {
        potentialChldrenInfo.add(new PotentialChildrenInfo(potentialChildren, op));
    }
    public void updateFDDependencies() {
        for (PotentialChildrenInfo pChildInfo : potentialChldrenInfo) {
            if (pChildInfo.childOp.isNonDeterministicOperation) {
                addParent(pChildInfo.childOp, pChildInfo.potentialChildOC, this);
                this.addDependencyForNondeterministicOperation(pChildInfo.childOp, pChildInfo.potentialChildOC, this);
            } else {
                addParent(pChildInfo.childOp, pChildInfo.potentialChildOC, this);
            }
        }
        potentialChldrenInfo.clear();
    }

    public void addParent(Operation childOp, OperationChain childOC, OperationChain parentOC) {
        Iterator<Operation> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            Operation parentOp = iterator.next();
            if (parentOp.bid < childOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                // setup dependencies on op level first.
                childOp.addFDParent(parentOp);
                setupDependency(childOp, childOC, parentOC, parentOp);
                break;
            }
        }
    }
    public void addDependencyForNondeterministicOperation(Operation parentOp, OperationChain parentOC, OperationChain childOC) {
        for (Operation childOp : this.getOperations()) {
            if (childOp.bid > parentOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                // setup dependencies on op level first.
                childOp.addFDParent(parentOp);
                childOC.setupDependency(childOp, childOC, parentOC, parentOp);
                break;
            }
        }
    }

    protected void setupDependency(Operation childOp, OperationChain childOC, OperationChain parentOC, Operation parentOp) {
        assert parentOC.getOperations().size() > 0;
        if (childOC.ocParents.putIfAbsent(parentOC, parentOp) == null) {
            childOC.ocParentsCount.incrementAndGet(); // there might have mulitple operations dependent on the same oc, eliminate those redundant here.
        }
        // add child for parent OC
        parentOC.ocChildren.put(this, childOp);
        assert childOC.ocParents.containsKey(parentOC);
        assert parentOC.ocChildren.containsKey(childOC);
    }

    public boolean isCircular(OperationChain parentOC) {
        boolean isCircular = false;
        if (parentOC.ocParents.containsKey(this)) {
            isCircular = true;
        }
        return isCircular;
    }

    public boolean isCircularAffected(HashSet<OperationChain> scannedOCs, HashSet<OperationChain> circularOCs) {
        for (OperationChain parent : ocParents.keySet()) {
            if (!scannedOCs.contains(parent)) { // if the oc is not traversed before, no circular.
                scannedOCs.add(parent);
                if (parent.isCircularAffected(scannedOCs, circularOCs)) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public void dfs(OperationChain oc, HashSet<OperationChain> affectedOCs) {
        affectedOCs.add(oc);
        for (OperationChain childOC : oc.ocChildren.keySet()) {
            if (!affectedOCs.contains(childOC)) {
                dfs(childOC, affectedOCs);
            }
        }
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + " " + bid + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationChain that = (OperationChain) o;
        return tableName.equals(that.tableName) &&
                primaryKey.equals(that.primaryKey) && bid == that.bid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, primaryKey, bid);
    }

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }

    public <T extends OperationChain> Collection<T> getParents() {
        return (Collection<T>) ocParents.keySet();
    }

    public <T extends OperationChain> Collection<T> getChildren() {
        return (Collection<T>) ocChildren.keySet();
    }

    public boolean hasParents() {
        return ocParentsCount.get() > 0;
    }

    public void setupTPG(TaskPrecedenceGraph tpg) {
//        this.tpg = tpg;
    }

    public void reset() {
        isExecuted = false;
        ocParentsCount.set(ocParents.size());
    }

    public void clear() {
        potentialChldrenInfo.clear();
        if (operations.size() != 0) {
//            operations.first().d_record.content_.clean_map(); //Disabled GC for ED
            operations.clear();
        }
        ocParents.clear();
        ocChildren.clear();
        isExecuted = false;
        // Non-structured data structure clearance
        needAbortHandling = false;
        failedOperations.clear();
        // Structured data structure clearance
        isDependencyLevelCalculated = false;
        dependencyLevel = -1;
    }

    // ------------------ Non-Structured Methods ------------------------
    public void setContext(OGSchedulerContext context) {
        if (this.context == null) {
            this.context = context;
        }
    }

    public void updateDependency() {
        ocParentsCount.decrementAndGet();
    }

    public boolean hasChildren() {
        return !ocChildren.isEmpty();
    }

    public void rollbackDependency() {
        ocParentsCount.incrementAndGet();
    }

    // ------------------ Structured Methods ------------------------
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
        for (OperationChain parent : getParents()) {
            if (!parent.hasValidDependencyLevel()) {
                parent.updateDependencyLevel();
            }

            if (parent.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = parent.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
    }

    public class PotentialChildrenInfo implements Comparable<PotentialChildrenInfo> {
        public OperationChain potentialChildOC;
        public Operation childOp;

        public PotentialChildrenInfo(OperationChain oc, Operation op) {
            this.potentialChildOC = oc;
            this.childOp = op;
        }

        @Override
        public int compareTo(PotentialChildrenInfo o) {
            return Double.compare(this.childOp.bid, o.childOp.bid);
        }
    }
}
