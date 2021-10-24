package scheduler.struct;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import profiler.MeasureTools;
import transaction.impl.ordered.MyList;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain<ExecutionUnit extends AbstractOperation> implements Comparable<OperationChain<ExecutionUnit>> {
    public final String tableName;
    public final String primaryKey;
    public final long bid;
    protected final MyList<ExecutionUnit> operations;
    public final AtomicInteger ocParentsCount;
    // OperationChain -> ChildOp that depend on the parent OC in cur OC
    public final ConcurrentHashMap<OperationChain<ExecutionUnit>, ExecutionUnit> ocParents;
    public final ConcurrentHashMap<OperationChain<ExecutionUnit>, ExecutionUnit> ocChildren;
    private final ConcurrentLinkedQueue<PotentialChildrenInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();
    public boolean isExecuted = false;

    protected TaskPrecedenceGraph tpg;

//    private final HashSet<OperationChain<ExecutionUnit>> scanedOCs = new HashSet<>();

    public OperationChain(String tableName, String primaryKey, long bid) {
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

    public void addOperation(ExecutionUnit op) {
        operations.add(op);
    }

    public void addPotentialFDChildren(OperationChain<ExecutionUnit> potentialChildren, ExecutionUnit op) {
        potentialChldrenInfo.add(new PotentialChildrenInfo(potentialChildren, op));
    }

    public void addParent(ExecutionUnit targetOp, OperationChain<ExecutionUnit> parentOC) {
        Iterator<ExecutionUnit> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            ExecutionUnit parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                // setup dependencies on op level first.
                targetOp.addFDParent(parentOp);
                setupDependency(targetOp, parentOC, parentOp);
                break;
            }
        }
    }

    protected void setupDependency(ExecutionUnit targetOp, OperationChain<ExecutionUnit> parentOC, ExecutionUnit parentOp) {
//        if (circularDetection(targetOp, parentOC, parentOp)) return;
        assert parentOC.getOperations().size() > 0;
        if (this.ocParents.putIfAbsent(parentOC, parentOp) == null) {
            this.ocParentsCount.incrementAndGet(); // there might have mulitple operations dependent on the same oc, eliminate those redundant here.
        }
        // add child for parent OC
        parentOC.ocChildren.put(this, targetOp);
        assert this.ocParents.containsKey(parentOC);
        assert parentOC.ocChildren.containsKey(this);
//        assert this.ocParents.size() == this.ocParentsCount.get();
    }

//    private boolean circularDetection(ExecutionUnit targetOp, OperationChain<ExecutionUnit> parentOC, ExecutionUnit parentOp) {
//        boolean isCircular;
//        // loop to find the circular
//        isCircular = isCircular(parentOC);
//        if (isCircular) { // if circular detected, try to solve circular
//            // TODO: create a new OC and put all ops after circular OP to the new OC.
//            OperationChain<ExecutionUnit> newOC = tpg.getNewOC(targetOp.table_name, targetOp.d_record.record_.GetPrimaryKey(), targetOp.bid);
//            List<ExecutionUnit> opsToMigrate = new ArrayList<>();
//            for (ExecutionUnit op : operations) {
//                if (op.bid >= targetOp.bid) {
//                    opsToMigrate.add(op);
//                }
//            }
//            opsToMigrate.forEach(operations::remove);
//            for (ExecutionUnit op : opsToMigrate) {
//                newOC.addOperation(op);
//            }
//            newOC.potentialChldrenInfo.addAll(this.potentialChldrenInfo); // move the potentialChildrenInfo to future
//            newOC.setupDependency(targetOp, this, this.getOperations().last());
//            newOC.setupDependency(targetOp, parentOC, parentOp);
//            return true;
//        }
//        return false;
//    }

    public boolean isCircular(OperationChain<ExecutionUnit> parentOC) {
        boolean isCircular = false;
        if (parentOC.ocParents.containsKey(this)) {
            isCircular = true;
        }
//        else {
//            scanedOCs.clear();
//            Collection<OperationChain<ExecutionUnit>> selectedOCs = parentOC.ocParents.keySet();
//            isCircular = scanParentOCs(selectedOCs);
//        }
        return isCircular;
    }

    public boolean isCircularAffected(HashSet<OperationChain<ExecutionUnit>> scannedOCs, HashSet<OperationChain<ExecutionUnit>> circularOCs) {
        for (OperationChain<ExecutionUnit> parent : ocParents.keySet()) {
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

//    private void relaxDependencies(OperationChain<ExecutionUnit> oc, ArrayDeque<OperationChain<ExecutionUnit>> resolvedOC) {
//        // remove all parents, update children set of its parents
////        for (OperationChain<ExecutionUnit> parent : oc.ocParents.keySet()) {
////            parent.ocChildren.remove(oc);
////        }
////        oc.ocParentsCount.set(0);
////        oc.ocParents.clear();
//        for (OperationChain<ExecutionUnit> child : oc.ocChildren.keySet()) {
//            if (!resolvedOC.contains(child)) {
//                resolvedOC.add(child);
//                relaxDependencies(child, resolvedOC);
//            }
//        }
//    }


//    public boolean scanParentOCs(Collection<OperationChain<ExecutionUnit>> selectedOCs) {
//        for (OperationChain<ExecutionUnit> oc : selectedOCs) {
//            if (!oc.ocParents.isEmpty() && !scanedOCs.contains(oc)) {
//                scanedOCs.add(oc);
//                if (oc.ocParents.containsKey(this)) {
//                    return true;
//                }
//                if (scanParentOCs(oc.ocParents.keySet())) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }

//    public boolean checkConnectivity(Collection<OperationChain<ExecutionUnit>> selectedOCs) {
//        if (selectedOCs.isEmpty()) {
//            return true;
//        }
//        for (OperationChain<ExecutionUnit> oc : selectedOCs) {
//            if (oc.ocParents.isEmpty()) {
//                return true;
//            } else {
//                return checkConnectivity(oc.ocParents.keySet());
//            }
//        }
//        return false;
//    }

    public void checkPotentialFDChildrenOnNewArrival(ExecutionUnit newOp) {
        List<PotentialChildrenInfo> processed = new ArrayList<>();

        for (PotentialChildrenInfo pChildInfo : potentialChldrenInfo) {
            if (newOp.bid < pChildInfo.childOp.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pChildInfo.potentialChildOC.addParent(pChildInfo.childOp, this);
                processed.add(pChildInfo);
            }
        }
        potentialChldrenInfo.removeAll(processed);
        processed.clear();
    }

    public MyList<ExecutionUnit> getOperations() {
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
        OperationChain<ExecutionUnit> that = (OperationChain<ExecutionUnit>) o;
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

    public <ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> boolean hasParents() {
        return ocParentsCount.get() > 0;
    }

    public void setupTPG(TaskPrecedenceGraph tpg) {
        this.tpg = tpg;
    }

    public class PotentialChildrenInfo implements Comparable<PotentialChildrenInfo> {
        public OperationChain<ExecutionUnit> potentialChildOC;
        public ExecutionUnit childOp;

        public PotentialChildrenInfo(OperationChain<ExecutionUnit> oc, ExecutionUnit op) {
            this.potentialChildOC = oc;
            this.childOp = op;
        }

        @Override
        public int compareTo(PotentialChildrenInfo o) {
            return Long.compare(this.childOp.bid, o.childOp.bid);
        }
    }
}
