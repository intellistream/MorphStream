package scheduler.struct;

import transaction.impl.ordered.MyList;

import java.util.*;
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

    private final ConcurrentLinkedQueue<PotentialDependencyInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();

    private final MyList<Operation> operations;
    private final AtomicInteger ocFdParentsCount;
    // OperationChainKey -> OperationChain
    private final ConcurrentSkipListMap<OperationChain, Operation> ocFdParents;
    private final ConcurrentSkipListMap<OperationChain, Operation> ocFdChildren;
    public boolean isExecuted = false;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;

        this.operations = new MyList<>(tableName, primaryKey);

        this.ocFdParentsCount = new AtomicInteger(0);

        this.ocFdParents = new ConcurrentSkipListMap<>();
        this.ocFdChildren = new ConcurrentSkipListMap<>();
    }

    public String getTableName() {
        return tableName;
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
                this.ocFdParents.putIfAbsent(parentOC, parentOp);
                this.ocFdParentsCount.incrementAndGet();
                parentOp.addChild(targetOp, MetaTypes.DependencyType.FD);
                parentOC.addFDChild(this, targetOp);
                break;
            }
        }
    }

    private void addFDChild(OperationChain childOC, Operation childOp) {
        this.ocFdChildren.putIfAbsent(childOC, childOp);
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

    public void updateDependency() {
        ocFdParentsCount.decrementAndGet();
    }

    public Collection<OperationChain> getFDChildren() {
        return ocFdChildren.keySet();
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
