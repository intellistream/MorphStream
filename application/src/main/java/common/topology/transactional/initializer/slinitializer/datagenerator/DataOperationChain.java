package common.topology.transactional.initializer.slinitializer.datagenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class DataOperationChain {

    private String stateId;
    private int operationsCount = 0;
    private int dependencyLevel;

    private boolean isDependencyLevelDirty =false;
    private boolean readForTraversal = false;

    private ArrayList<DataOperationChain> dependsUpon = new ArrayList<>();
    private ArrayList<DataOperationChain> dependents = new ArrayList<>();

    private HashMap<Integer, ArrayList<DataOperationChain>> operationChainsByLevel;

    public DataOperationChain(String stateId, HashMap<Integer, ArrayList<DataOperationChain>> operationChainsByLevel) {
        this.stateId = stateId;
        this.operationChainsByLevel = operationChainsByLevel;
        updateLevelInMap();
    }
    public String getStateId() {
        return stateId;
    }
    public int getId() {
        return Integer.parseInt(stateId.split("_")[1]);
    }

    public int getOperationsCount() {
        return operationsCount;
    }

    public void markReadyForTraversal() {
        readForTraversal = true;
    }

    private void updateLevelInMap() {
        if(!operationChainsByLevel.containsKey(getDependencyLevel())) {
            operationChainsByLevel.put(getDependencyLevel(), new ArrayList<>());
        }
        operationChainsByLevel.get(getDependencyLevel()).add(this);
    }

    private void removeFromMap() {
        if(null != operationChainsByLevel.get(getDependencyLevel())) {
            operationChainsByLevel.get(getDependencyLevel()).remove(this);
        }
    }

    public void registerAllDependenciesToList( ArrayList<String> allDependencies) {
        if(!readForTraversal)
            return;
        readForTraversal = false;
        for(DataOperationChain oc: dependsUpon) {
            String dependencyName = String.format("%s,%s", stateId, oc.getStateId());
            if(!allDependencies.contains(dependencyName))
                allDependencies.add(dependencyName);
            oc.registerAllDependenciesToList(allDependencies);
        }
    }

    public ArrayList<String> getDependencyChainInfo() {
        ArrayList<String> chains = new ArrayList<>();
        for(DataOperationChain oc: dependsUpon) {
            ArrayList<String> dependsUponChains = oc.getDependencyChainInfo();
            if(dependsUponChains.size()==0)
                chains.add(stateId+"->"+oc.getStateId());
            else
                for(String dependsUponChain: dependsUponChains)
                    chains.add(stateId+"->"+dependsUponChain);
        }
        return chains;
    }

    public void addDependency(DataOperationChain dependency) {
        dependsUpon.add(dependency);
    }

    public void addDependent(DataOperationChain dependent) {
        dependents.add(dependent);
    }

    public boolean hasInAllDependents(DataOperationChain oc) {
        if(!readForTraversal)
            return false;
        readForTraversal = false;

        boolean traversalResult = false;
        for (DataOperationChain doc: dependents) {
            traversalResult |= oc.equals(doc);
            if(traversalResult) break;

            traversalResult |= doc.hasInAllDependents(oc);
            if(traversalResult) break;
        }
        return traversalResult;
    }

    public boolean hasDependents(){
        return !dependents.isEmpty();
    }

    public void markAllDependencyLevelsDirty() {
        if(isDependencyLevelDirty)
            return;
        isDependencyLevelDirty = true;
        removeFromMap();
        for (DataOperationChain doc: dependents) {
            doc.markAllDependencyLevelsDirty();
        }
    }

    public void updateAllDependencyLevel() {
        if(!isDependencyLevelDirty)
            return;
        updateDependencyLevel();
        isDependencyLevelDirty = false;
        updateLevelInMap();

        for (DataOperationChain doc: dependents) {
            doc.updateAllDependencyLevel();
        }
    }

    private void updateDependencyLevel() {
        dependencyLevel = 0;
        for(DataOperationChain oc: dependsUpon) {
            if(oc.getDependencyLevel()>=dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel()+1;
            }
        }
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    public void addAnOperation() {
        operationsCount += 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataOperationChain that = (DataOperationChain) o;
        return Objects.equals(stateId, that.stateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateId);
    }
}
