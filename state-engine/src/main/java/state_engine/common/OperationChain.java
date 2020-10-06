package state_engine.common;

import state_engine.transaction.dedicated.ordered.MyList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationChain implements Comparable<OperationChain>{

    private String tableName;
    private String primaryKey;

    private MyList<Operation> operations;
    private ConcurrentSkipListSet<OperationChain> dependsUpon;
    private ConcurrentSkipListSet<DependentInfo> potentialDependentsInfo; // for delayed events, to check if some other operation chain depends upon this one.

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);

        this.dependsUpon = new ConcurrentSkipListSet<>();
        this.potentialDependentsInfo = new ConcurrentSkipListSet<>();
    }

    public void addOperation(Operation op) {
        this.operations.add(op);
    }

    public MyList<Operation> getOperations() {
        return operations;
    }

    public void addDependency(OperationChain dependsUpon) {
        if(!this.dependsUpon.contains(dependsUpon))
            this.dependsUpon.add(dependsUpon);
    }

    public void addPotentialDependent(OperationChain potentialDependent, long bid) {
        DependentInfo pdInfo = getPotentialDependentInfo(potentialDependent);
        if(pdInfo!=null)
            pdInfo.bid = bid;
        else
            potentialDependentsInfo.add(new DependentInfo(potentialDependent, bid));
    }

    private DependentInfo getPotentialDependentInfo(OperationChain potentialDependent) {
        for(DependentInfo dependentInfo : potentialDependentsInfo) { // if potentialDependent is already in list, then just update info with new ops bid.
            if(dependentInfo.equals(potentialDependent)) {
                return dependentInfo;
            }
        }
        return null;
    }



    public void checkOtherPotentialDependencies(long dependencyBid) {
        List<DependentInfo> processed = new ArrayList<>();
        for(DependentInfo dependentInfo : potentialDependentsInfo) {
            if(dependencyBid < dependentInfo.bid) { // if bid is < dependents bid, therefore, it depends upon this operation
                dependentInfo.dependent.addDependency(this);
                processed.add(dependentInfo);
            }
        }

        for(DependentInfo dependentInfo : processed) {
            potentialDependentsInfo.remove(dependentInfo);
        }
        processed.clear();

    }

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    public int dependencyLevel = -1;

    public synchronized void updateDependencyLevel() {

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

    public boolean hasValidDependencyLevel(){
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

    public void printPotentialDependencies() {
        System.out.println("Potential Dependencies");
        for (DependentInfo opChainInfo: potentialDependentsInfo) {
            System.out.println(opChainInfo.dependent.toString());
        }
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

    private static class DependentInfo implements Comparable<DependentInfo> {
        public OperationChain dependent;
        public long bid;

        public DependentInfo(OperationChain dependent, long bid) {
            this.dependent = dependent;
            this.bid = bid;
        }

        @Override
        public int compareTo(DependentInfo o) {
            if(dependent.equals(o.dependent) && this.bid == o.bid)
                return 0; // Let the order be, in which they are added.
            else
                return -1;
        }
    }

}
