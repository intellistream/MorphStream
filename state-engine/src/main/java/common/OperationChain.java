package common;

import transaction.dedicated.ordered.MyList;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationChain implements Comparable<OperationChain>, Operation.IOpConflictResolutionListener {

    private String tableName;
    private String primaryKey;

    public OperationChain next;
    public OperationChain prev;

    private MyList<Operation> operations;
    private ConcurrentSkipListMap<OperationChain, Operation> dependsUpon;
    private AtomicInteger totalDependentsCount = new AtomicInteger();
    private AtomicInteger totalDependenciesCount = new AtomicInteger();

    private int minimumIndependentOpsCount = Integer.MAX_VALUE;
    private int dependencyLevel = -1;
    private int priority = 0;

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
            if(dependencyOp.bid < forOp.bid) {
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

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    public synchronized void updateDependencyLevel( ) {

        if(isDependencyLevelCalculated)
            return;

        dependencyLevel = 0;
        for (OperationChain oc: dependsUpon.keySet()) {

            if(!oc.hasValidDependencyLevel())
                oc.updateDependencyLevel();

            if(oc.getDependencyLevel()>=dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel()+1;
            }
        }
        isDependencyLevelCalculated = true;
    }

    public synchronized boolean hasValidDependencyLevel(){
        return isDependencyLevelCalculated;
    }

    private IOnDependencyResolvedListener onDependencyResolvedListener;
    public void setOnOperationChainChangeListener(IOnDependencyResolvedListener onDependencyResolvedListener) {
        this.onDependencyResolvedListener = onDependencyResolvedListener;
    }

    @Override
    public void onDependencyResolved(int threadId, Operation dependent, Operation dependency) {

        if(totalDependenciesCount.decrementAndGet()==0)
            if(onDependencyResolvedListener!=null)
                onDependencyResolvedListener.onDependencyResolvedListener(threadId, this);
    }

    @Override
    public void onDependentResolved(Operation dependent, Operation dependency) {
        totalDependentsCount.decrementAndGet();
    }

    public boolean hasDependency() {
        return totalDependenciesCount.get()>0;
    }

    public boolean hasDependents() {
        return totalDependentsCount.get()>0;
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

    private ConcurrentLinkedQueue<PotentialDependencyInfo> potentialDependentsInfo = new ConcurrentLinkedQueue<>();

    public void addPotentialDependent(OperationChain potentialDependent, Operation op) {
        potentialDependentsInfo.add(new PotentialDependencyInfo(potentialDependent, op));
    }

    public void checkOtherPotentialDependencies(Operation dependencyOp) {

        List<PotentialDependencyInfo> processed = new ArrayList<>();

        for(PotentialDependencyInfo pDependentInfo : potentialDependentsInfo) {
            if(dependencyOp.bid < pDependentInfo.op.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
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
        return "{" + tableName + " " + primaryKey+ "}";//": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
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
        if(tableName.contains("accounts")) {
            return String.format("act_%s",primaryKey);
        } else {
            return String.format("ast_%s",primaryKey);
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
        if(o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }


    public interface IOnDependencyResolvedListener {
        void onDependencyResolvedListener(int threadId, OperationChain oc);
    }

}
