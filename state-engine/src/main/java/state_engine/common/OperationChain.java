package state_engine.common;

import state_engine.transaction.dedicated.ordered.MyList;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationChain implements Comparable<OperationChain>{

    private String tableName;
    private String primaryKey;

    private MyList<Operation> operations;
    private ArrayList<OperationChain> dependsUpon;
    private HashMap<OperationChain, Long> potentialDependentsInfo;

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

    public synchronized void addDependency(OperationChain dependsUpon) {
        if(!this.dependsUpon.contains(dependsUpon))
            this.dependsUpon.add(dependsUpon);
    }

    public synchronized void addPotentialDependent(OperationChain potentialDependent, long bid) {
        potentialDependentsInfo.put(potentialDependent, bid);
    }

    public synchronized void checkOtherPotentialDependencies(long dependencyBid) {
        List<OperationChain> processed = new ArrayList<>();
        for(OperationChain pDependent : potentialDependentsInfo.keySet()) {
            if(dependencyBid < potentialDependentsInfo.get(pDependent)) { // if bid is < dependents bid, therefore, it depends upon this operation
                pDependent.addDependency(this);
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
