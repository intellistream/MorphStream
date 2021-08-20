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
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class OperationChain<ExecutionUnit extends AbstractOperation> implements Comparable<OperationChain> {
    public final String tableName;
    public final String primaryKey;

    private final ConcurrentLinkedQueue<PotentialChildrenInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();

    private final MyList<ExecutionUnit> operations;
    protected final AtomicInteger ocFdParentsCount;
    // OperationChainKey -> OperationChain
    protected final ConcurrentSkipListMap<OperationChain, ExecutionUnit> ocFdParents;
    public boolean isExecuted = false;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
        this.ocFdParentsCount = new AtomicInteger(0);
        this.ocFdParents = new ConcurrentSkipListMap<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void addOperation(ExecutionUnit op) {
        operations.add(op);
    }

    public void addPotentialFDChildren(OperationChain potentialChildren, ExecutionUnit op) {
        potentialChldrenInfo.add(new PotentialChildrenInfo(potentialChildren, op));
    }

    public void addFDParent(ExecutionUnit targetOp, OperationChain parentOC) {
        Iterator<ExecutionUnit> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            ExecutionUnit parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                setupDependency(targetOp, parentOC, parentOp);
                break;
            }
        }
    }

    protected void setupDependency(ExecutionUnit targetOp, OperationChain parentOC, ExecutionUnit parentOp) {
        this.ocFdParents.putIfAbsent(parentOC, parentOp);
        this.ocFdParentsCount.incrementAndGet();
    }

    public void checkPotentialFDChildrenOnNewArrival(ExecutionUnit newOp) {
        List<PotentialChildrenInfo> processed = new ArrayList<>();

        for (PotentialChildrenInfo pChildInfo : potentialChldrenInfo) {
            if (newOp.bid < pChildInfo.childOp.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pChildInfo.potentialChildOC.addFDParent(pChildInfo.childOp, (OperationChain) this);
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


    public boolean hasParents() {
        return ocFdParentsCount.get() > 0;
    }

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
        for (OperationChain oc : ocFdParents.keySet()) {
            if (!oc.hasValidDependencyLevel())
                oc.updateDependencyLevel();

            if (oc.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
    }

    public class PotentialChildrenInfo implements Comparable<PotentialChildrenInfo> {
        public OperationChain potentialChildOC;
        public ExecutionUnit childOp;

        public PotentialChildrenInfo(OperationChain oc, ExecutionUnit op) {
            this.potentialChildOC = oc;
            this.childOp = op;
        }

        @Override
        public int compareTo(PotentialChildrenInfo o) {
            return Long.compare(this.childOp.bid, o.childOp.bid);
        }
    }
}
