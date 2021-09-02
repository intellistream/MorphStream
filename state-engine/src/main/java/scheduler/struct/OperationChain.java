package scheduler.struct;

import transaction.impl.ordered.MyList;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain<ExecutionUnit extends AbstractOperation> implements Comparable<OperationChain<ExecutionUnit>> {
    public final String tableName;
    public final String primaryKey;
    protected final MyList<ExecutionUnit> operations;
    protected final AtomicInteger ocParentsCount;
    // OperationChain -> ChildOp that depend on the parent OC in cur OC
    public final ConcurrentSkipListMap<OperationChain<ExecutionUnit>, ExecutionUnit> ocParents;
    public final ConcurrentSkipListMap<ExecutionUnit, OperationChain<ExecutionUnit>> targetOpToOcParents;
    private final ConcurrentLinkedQueue<PotentialChildrenInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();
    public boolean isExecuted = false;
    protected TaskPrecedenceGraph tpg;
    Set<ExecutionUnit> circularOps = new HashSet<>(2);
    HashMap<ExecutionUnit, OperationChain<ExecutionUnit>> circularParents = new HashMap<>();


    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
        this.ocParentsCount = new AtomicInteger(0);
        this.ocParents = new ConcurrentSkipListMap<>();
        this.targetOpToOcParents = new ConcurrentSkipListMap<>();
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
                setupDependency(targetOp, parentOC, parentOp);
                break;
            }
        }
    }

    protected void setupDependency(ExecutionUnit targetOp, OperationChain<ExecutionUnit> parentOC, ExecutionUnit parentOp) {
        this.ocParents.putIfAbsent(parentOC, parentOp);
        this.targetOpToOcParents.putIfAbsent(targetOp, parentOC);
        this.ocParentsCount.incrementAndGet();
        if (parentOC.ocParents.containsKey(this)) {
            ExecutionUnit circularSrcOp = parentOC.ocParents.get(this);
            circularOps.add(circularSrcOp); // add the previous op in this oc that caused circular
            circularOps.add(targetOp); // add current op in this oc that caused circular
            circularParents.put(circularSrcOp, parentOC);
            tpg.cirularOCs.add(this);

//            throw new RuntimeException("cyclic in the tpg;");
        }
    }

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
        return "{" + tableName + " " + primaryKey + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationChain<ExecutionUnit> that = (OperationChain<ExecutionUnit>) o;
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

    public <T extends OperationChain> Collection<T> getParents() {
        return (Collection<T>) ocParents.keySet();
    }

    public <ExecutionUnit extends AbstractOperation, SchedulingUnit extends OperationChain<ExecutionUnit>> boolean hasParents() {
        return ocParentsCount.get() > 0;
    }

    public void setupTPG(TaskPrecedenceGraph tpg) {
        this.tpg = tpg;
    }

    public void clear() {
        operations.clear();
        ocParentsCount.set(0);
        ocParents.clear();
        targetOpToOcParents.clear();
        potentialChldrenInfo.clear();
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
