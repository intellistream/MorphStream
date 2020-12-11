package state_engine.common;

import state_engine.transaction.dedicated.ordered.MyList;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationChain implements Comparable<OperationChain>, Operation.IOpDependenciesResolutionListener {

    private String tableName;
    private String primaryKey;

    private MyList<Operation> operations;
    private ArrayList<OperationChain> dependsUpon;

    private HashMap<OperationChain, Operation> potentialDependentsInfo;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    public int dependencyLevel = -1;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);

        this.dependsUpon = new ArrayList<>();
        this.potentialDependentsInfo = new HashMap<>();
    }

    public void addOperation(Operation op) {
        this.operations.add(op);
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    public synchronized void addDependency(Operation forOp, OperationChain dependsUpon) {
        if(!this.dependsUpon.contains(dependsUpon))
            this.dependsUpon.add(dependsUpon);

        Iterator<Operation> iterator = dependsUpon.getOperations().descendingIterator();
        while (iterator.hasNext()) {
            Operation dependencyOp = iterator.next();
            if(dependencyOp.bid < forOp.bid) {
                forOp.addDependency(this, dependencyOp);
                dependencyOp.addDependent(forOp);
                addOpDependency(dependencyOp, forOp);
                dependsUpon.addOpDependent(dependencyOp, forOp);
                break;
            }
        }
    }

    private ConcurrentSkipListMap<Operation, List<Operation>> opDependsUpon = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Operation, List<Operation>> opDependents = new ConcurrentSkipListMap<>();
    private int minimumIndependentOpsCount = Integer.MAX_VALUE;

    private void addOpDependency(Operation dependencyOp, Operation dependentOp) {
        if(!opDependsUpon.containsKey(dependentOp))
            opDependsUpon.put(dependentOp, new ArrayList<>());
        opDependsUpon.get(dependentOp).add(dependencyOp); // we want to know about the dependency op.
    }

    private void addOpDependent(Operation dependencyOp, Operation dependentOp) {
        if(!opDependents.containsKey(dependencyOp))
            opDependents.put(dependencyOp, new ArrayList<>());
        opDependents.get(dependencyOp).add(dependentOp); // we want to know about the dependent of our dependency Op.
    }

    @Override
    public void onDependenciesResolved(Operation opFor) {
        opDependsUpon.remove(opFor);
    }

    public void getMinimumIndependentOpsCount() { // maintain an efficient map of position of dependent ops instead.
        Iterator<Operation> opsItr = operations.iterator();
        int independentOpsCount = 0;
        while(opsItr.hasNext()) {
            if (opDependsUpon.containsKey(opsItr.next()))
                break;
            independentOpsCount += 1;
        }
    }

    public synchronized void addPotentialDependent(OperationChain potentialDependent, Operation op) {
        potentialDependentsInfo.put(potentialDependent, op);
    }

    public synchronized void checkOtherPotentialDependencies(Operation dependencyOp) {

        List<OperationChain> processed = new ArrayList<>();
        for(OperationChain pDependent : potentialDependentsInfo.keySet()) {
            if(dependencyOp.bid < potentialDependentsInfo.get(pDependent).bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                pDependent.addDependency(potentialDependentsInfo.get(pDependent), this);
                processed.add(pDependent);
            }
        }

        for(OperationChain pDependent : processed)
            potentialDependentsInfo.remove(pDependent);

        processed.clear();

    }

    public synchronized void updateDependencyLevel( ) {

        if(isDependencyLevelCalculated)
            return;

        dependencyLevel = 0;
        for (OperationChain oc: dependsUpon) {

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

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey + '}';
    }

    public void printDependencies() {
        System.out.print("Dependencies of "+toString()+" --> ");
        for (OperationChain opChain: dependsUpon) {
            System.out.print(opChain.toString());
        }
        System.out.print(",    Level: "+getDependencyLevel());
        System.out.println(" ");
    }

    public void addAllDependencies(ArrayList<String> dependenciesContainer) {
        for (OperationChain opChain: dependsUpon) {
            String dependency = String.format("%s, %s", getStringId(), opChain.getStringId());
            if(!dependenciesContainer.contains(dependency)) {
                dependenciesContainer.add(dependency);
            }
        }
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

    private boolean isProcessed;
    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed() {
        isProcessed = true;
    }


//    private static class DependentInfo implements Comparable<DependentInfo> {
//        public OperationChain dependent;
//        public long bid;
//
//        public DependentInfo(OperationChain dependent, long bid) {
//            this.dependent = dependent;
//            this.bid = bid;
//        }
//
//        @Override
//        public int compareTo(DependentInfo o) {
//            if(dependent.equals(o.dependent) && this.bid == o.bid)
//                return 0; // Let the order be, in which they are added.
//            else
//                return -1;
//        }
//    }

}
