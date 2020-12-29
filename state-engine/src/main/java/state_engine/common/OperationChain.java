package state_engine.common;

import state_engine.transaction.dedicated.ordered.MyList;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;

public class OperationChain implements Comparable<OperationChain>, Operation.IOpConflictResolutionListener {

    private String tableName;
    private String primaryKey;

    private MyList<Operation> operations;

    private int totalOpsCount = 0;
    private int totalDependentsCount = 0;
    private int totalDependenciesCount = 0;

    private ConcurrentHashMap<OperationChain, Operation> potentialDependentsInfo;
    private ConcurrentHashMap<OperationChain, Queue<Operation>> dependsUpon;
    private ConcurrentHashMap<OperationChain, Queue<Operation>> dependents;

    private int minimumIndependentOpsCount = Integer.MAX_VALUE;
    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;
    private int priority = 0;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);

        this.dependsUpon = new ConcurrentHashMap<>();
        this.dependents = new ConcurrentHashMap<>();
        this.potentialDependentsInfo = new ConcurrentHashMap<>();
    }

    public void addOperation(Operation op) {
        op.setOc(this);
        this.operations.add(op);
        if(operations.size()>totalOpsCount)
            totalOpsCount = operations.size();
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

    public synchronized void addDependency(Operation forOp, OperationChain dependsUpon) {

        Iterator<Operation> iterator = dependsUpon.getOperations().descendingIterator(); // we want to get op with largest bid which is smaller than forOp bid
        while (iterator.hasNext()) {
            Operation dependencyOp = iterator.next();
            if(dependencyOp.bid < forOp.bid) {
                // record dependency
                forOp.addDependency(this, dependencyOp); // I(oc) listen for my dependencies resolution.
                addDependency(dependsUpon, dependencyOp);
                // record dependents
                dependencyOp.addDependent(dependsUpon, forOp);
                dependsUpon.addDependent(this, forOp);
                break;
            }
        }
    }

    private void addDependency(OperationChain dependencyOc, Operation dependencyOp) {
        if(!dependsUpon.contains(dependencyOc))
            dependsUpon.putIfAbsent(dependencyOc, new ConcurrentLinkedQueue<>());
        dependsUpon.get(dependencyOc).add(dependencyOp);
        if(totalDependenciesCount<dependsUpon.size())
            totalDependenciesCount = dependsUpon.size();
    }

    private void addDependent(OperationChain dependentOc, Operation dependentOp) {
        if(!dependents.contains(dependentOc))
            dependents.putIfAbsent(dependentOc, new ConcurrentLinkedQueue<>());
        dependents.get(dependentOc).add(dependentOp);
        if(totalDependentsCount<dependents.size())
            totalDependentsCount = dependents.size();
    }


    private IOnDependencyResolvedListener onDependencyResolvedListener;
    public void setOnOperationChainChangeListener(IOnDependencyResolvedListener onDependencyResolvedListener) {
        this.onDependencyResolvedListener = onDependencyResolvedListener;
    }

    public IOnDependencyResolvedListener getOnDependencyResolvedListener() {
        return onDependencyResolvedListener;
    }

    @Override
    public void onDependencyResolved(Operation dependent, Operation dependency) {

        Queue<Operation> dependencyOps = dependsUpon.get(dependency.getOc());
        dependencyOps.remove(dependency);
        if(dependencyOps.isEmpty()) {
            dependsUpon.remove(dependency.getOc());
        }

        if(dependsUpon.size()==0) {
            if(onDependencyResolvedListener!=null)
                onDependencyResolvedListener.onDependencyResolvedListener(this);
        }
    }

    @Override
    public void onDependentResolved(Operation dependent, Operation dependency) {
        Queue<Operation> dependentOps = dependents.get(dependent.getOc());
        dependentOps.remove(dependent);
        if(dependentOps.isEmpty()) {
            dependents.remove(dependent.getOc());
        }
    }


    public boolean hasDependency() {
        return dependsUpon.size()>0;
    }

    public boolean hasDependents() {
        return dependents.size()>0;
    }

    public synchronized void addPotentialDependent(OperationChain potentialDependent, Operation op) {
        potentialDependentsInfo.put(potentialDependent, op);
    }

    public synchronized void checkOtherPotentialDependencies(Operation dependencyOp) {

        List<OperationChain> processed = new ArrayList<>();
        ConcurrentHashMap.KeySetView<OperationChain, Operation> keys = potentialDependentsInfo.keySet();
        for(OperationChain pDependent : keys) {
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

    public int getDependencyLevel() {
        return dependencyLevel;
    }


    @Override
    public String toString() {
        return "{" + tableName + " " + primaryKey+ ": dependencies Count: "+dependsUpon.size()+ ": dependents Count: "+dependents.size()+ ": initialDependencyCount: "+totalDependenciesCount+ ": initialDependentsCount: "+totalDependentsCount+"}";
    }

    public void printDependencies() {
        String string = "";
        string += "Dependencies of "+toString()+" --> ";
        for (OperationChain opChain: dependsUpon.keySet()) {
            string += opChain.toString();
        }
        string += ",    Level: "+getDependencyLevel();
        System.out.println(string);
    }

    public String getDependenciesStr() {
        String str = "";
        str += "Dependencies of "+toString()+" --> ";
        for (OperationChain opChain: dependsUpon.keySet()) {
            str += opChain.toString();
        }
        str += ",    Level: "+getDependencyLevel();
        return str;
    }

    public void addAllDependencies(ArrayList<String> dependenciesContainer) {
        for (OperationChain opChain: dependsUpon.keySet()) {
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


    public interface IOnDependencyResolvedListener {
        void onDependencyResolvedListener(OperationChain oc);
    }

}
