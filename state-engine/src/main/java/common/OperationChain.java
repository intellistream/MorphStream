package common;

import transaction.dedicated.ordered.MyList;
import transaction.scheduler.obsolete.IOnDependencyResolvedListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: Aqif Hamid
 * An Operation Chain represents a single set of operations executed by a thread.
 */
public class OperationChain implements Comparable<OperationChain>, Operation.IOpConflictResolutionListener {

    private final String tableName;
    private final String primaryKey;
    private final MyList<Operation> operations;
    private final ConcurrentSkipListMap<OperationChain, Operation> dependsUpon;
    private final AtomicInteger totalDependentsCount = new AtomicInteger();
    private final AtomicInteger totalDependenciesCount = new AtomicInteger();
    private final int minimumIndependentOpsCount = Integer.MAX_VALUE;
    private final ConcurrentLinkedQueue<PotentialDependencyInfo> potentialDependentsInfo = new ConcurrentLinkedQueue<>();
    public OperationChain next;
    public OperationChain prev;
    private int dependencyLevel = -1;
    private int priority = 0;
    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private IOnDependencyResolvedListener onDependencyResolvedListener;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
        this.dependsUpon = new ConcurrentSkipListMap<>();
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void addOperation(Operation op) {
        op.setOc(this);
        operations.add(op);
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void addDependency(Operation forOp, OperationChain dependsUponOp) {
        Iterator<Operation> iterator = dependsUponOp.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than forOp bid
        while (iterator.hasNext()) {
            Operation dependencyOp = iterator.next();
            if (dependencyOp.bid < forOp.bid) {
                totalDependenciesCount.incrementAndGet();
                this.dependsUpon.putIfAbsent(dependsUponOp, dependencyOp);
                dependencyOp.addDependent(dependsUponOp, forOp);
                dependsUponOp.addDependent(this, forOp);
                break;
            }
        }
    }

    private void addDependent(OperationChain dependentOc, Operation dependentOp) {
        totalDependentsCount.incrementAndGet();
    }

    public synchronized void updateDependencyLevel() {

        if (isDependencyLevelCalculated)
            return;

        dependencyLevel = 0;
        for (OperationChain oc : dependsUpon.keySet()) {

            if (!oc.hasValidDependencyLevel())
                oc.updateDependencyLevel();

            if (oc.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
    }

    public synchronized boolean hasValidDependencyLevel() {
        return isDependencyLevelCalculated;
    }

    public void setOnOperationChainChangeListener(IOnDependencyResolvedListener onDependencyResolvedListener) {
        this.onDependencyResolvedListener = onDependencyResolvedListener;
    }

    @Override
    public void onDependencyResolved(int threadId, Operation dependent, Operation dependency) {

        if (totalDependenciesCount.decrementAndGet() == 0)
            if (onDependencyResolvedListener != null)
                onDependencyResolvedListener.onDependencyResolvedListener(threadId, this);
    }

    @Override
    public void onDependentResolved(Operation dependent, Operation dependency) {
        totalDependentsCount.decrementAndGet();
    }

    public boolean hasDependency() {
        return totalDependenciesCount.get() > 0;
    }

    public boolean hasDependents() {
        return totalDependentsCount.get() > 0;
    }

    public void addPotentialDependent(OperationChain potentialDependent, Operation op) {
        potentialDependentsInfo.add(new PotentialDependencyInfo(potentialDependent, op));
    }

    public void checkOtherPotentialDependencies(Operation dependencyOp) {

        List<PotentialDependencyInfo> processed = new ArrayList<>();

        for (PotentialDependencyInfo pDependentInfo : potentialDependentsInfo) {
            if (dependencyOp.bid < pDependentInfo.op.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pDependentInfo.oc.addDependency(pDependentInfo.op, this);
                processed.add(pDependentInfo);
            }
        }
        potentialDependentsInfo.removeAll(processed);
        processed.clear();
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    public void printDependencies() {
//        String string = "";
//        string += "Dependencies of "+toString()+" --> ";
//        for (OperationChain opChain: dependsUpon.keySet()) {
//            string += opChain.toString();
//        }
//        string += ",    Level: "+getDependencyLevel();
//        System.out.println(string);
    }

    public String getDependenciesStr() {
//        String str = "";
//        str += "Dependencies of "+toString()+" --> ";
//        for (OperationChain opChain: dependsUpon.keySet()) {
//            str += opChain.toString();
//        }
//        str += ",    Level: "+getDependencyLevel();
        return "";
    }

    public void addAllDependencies(ArrayList<String> dependenciesContainer) {
//        for (OperationChain opChain: dependsUpon.keySet()) {
//            String dependency = String.format("%s, %s", getStringId(), opChain.getStringId());
//            if(!dependenciesContainer.contains(dependency)) {
//                dependenciesContainer.add(dependency);
//            }
//        }
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

    public class PotentialDependencyInfo implements Comparable<PotentialDependencyInfo> {
        public OperationChain oc;
        public Operation op;

        public PotentialDependencyInfo(OperationChain oc, Operation op) {
            this.oc = oc;
            this.op = op;
        }

        @Override
        public int compareTo(PotentialDependencyInfo o) {
            return Long.compare(this.op.bid, o.op.bid);
        }
    }

}
