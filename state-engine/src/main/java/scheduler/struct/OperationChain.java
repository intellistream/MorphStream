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
public class OperationChain<OP extends AbstractOperation, OC extends OperationChain> implements Comparable<OC> {
    private String tableName;
    private String primaryKey;

    private final ConcurrentLinkedQueue<PotentialChildrenInfo> potentialChldrenInfo = new ConcurrentLinkedQueue<>();

    private MyList<OP> operations;
    private final AtomicInteger ocFdParentsCount = new AtomicInteger(0);
    // OperationChainKey -> OperationChain
    private final ConcurrentSkipListMap<OC, OP> ocFdParents = new ConcurrentSkipListMap<>();
    public boolean isExecuted = false;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.setTableName(tableName);
        this.setPrimaryKey(primaryKey);
        this.setOperations(new MyList<>(tableName, primaryKey));
    }

    public OperationChain() {}


    public void initialize(String tableName, String primaryKey) {
        this.setTableName(tableName);
        this.setPrimaryKey(primaryKey);
        this.setOperations(new MyList<>(tableName, primaryKey));
    }

    public String getTableName() {
        return tableName;
    }

    public void addOperation(OP op) {
        getOperations().add(op);
    }

    public void addPotentialFDChildren(OC potentialChildren, OP op) {
        potentialChldrenInfo.add(new PotentialChildrenInfo(potentialChildren, op));
    }

    public void addFDParent(OP targetOp, OC parentOC) {
        Iterator<OP> iterator = parentOC.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than targetOp bid
        while (iterator.hasNext()) {
            OP parentOp = iterator.next();
            if (parentOp.bid < targetOp.bid) { // find the exact operation in parent OC that this target OP depends on.
                setupDependency(targetOp, parentOC, parentOp);
                break;
            }
        }
    }

    protected void setupDependency(OP targetOp, OC parentOC, OP parentOp) {
        this.getOcFdParents().putIfAbsent(parentOC, parentOp);
        this.getOcFdParentsCount().incrementAndGet();
    }

    public void checkPotentialFDChildrenOnNewArrival(OP newOp) {
        List<PotentialChildrenInfo> processed = new ArrayList<>();

        for (PotentialChildrenInfo pChildInfo : potentialChldrenInfo) {
            if (newOp.bid < pChildInfo.childOp.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pChildInfo.potentialChildOC.addFDParent(pChildInfo.childOp, this);
                processed.add(pChildInfo);
            }
        }
        potentialChldrenInfo.removeAll(processed);
        processed.clear();
    }

    public MyList<OP> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "{" + getTableName() + " " + getPrimaryKey() + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OC that = (OC) o;
        return getTableName().equals(that.getTableName()) &&
                getPrimaryKey().equals(that.getPrimaryKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableName(), getPrimaryKey());
    }

    @Override
    public int compareTo(OC o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }


    public boolean hasParents() {
        return getOcFdParentsCount().get() > 0;
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
        for (OC oc : getOcFdParents().keySet()) {
            if (!oc.hasValidDependencyLevel())
                oc.updateDependencyLevel();

            if (oc.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setOperations(MyList<OP> operations) {
        this.operations = operations;
    }

    public AtomicInteger getOcFdParentsCount() {
        return ocFdParentsCount;
    }

    public ConcurrentSkipListMap<OC, OP> getOcFdParents() {
        return ocFdParents;
    }

    public class PotentialChildrenInfo implements Comparable<PotentialChildrenInfo> {
        public OC potentialChildOC;
        public OP childOp;

        public PotentialChildrenInfo(OC oc, OP op) {
            this.potentialChildOC = oc;
            this.childOp = op;
        }

        @Override
        public int compareTo(PotentialChildrenInfo o) {
            return Long.compare(this.childOp.bid, o.childOp.bid);
        }
    }
}
