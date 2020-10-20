package benchmark.datagenerator;

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
    private int averageRecordsPerDeLevel;

    public DataOperationChain(String stateId, int averageRecordsPerDeLevel, HashMap<Integer, ArrayList<DataOperationChain>> operationChainsByLevel) {
        this.stateId = stateId;
        this.operationChainsByLevel = operationChainsByLevel;
        this.averageRecordsPerDeLevel = averageRecordsPerDeLevel;
        updateLevelInMap(0, averageRecordsPerDeLevel);
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


    public void markReadyForTraversal() {
        readForTraversal = true;
    }

    public void markDependentsReadyForTraversal() {
        if(readForTraversal)
            return;
        readForTraversal = true;
        for (DataOperationChain doc: dependents) {
            doc.markDependentsReadyForTraversal();
        }
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

    public void markDependsUponReadyForTraversal() {
        if(readForTraversal)
            return;
        readForTraversal = true;
        for (DataOperationChain doc: dependsUpon) {
            doc.markDependentsReadyForTraversal();
        }
    }
    public boolean doesDependsUpon(DataOperationChain oc) {
        boolean traversalResult = false;
        for (DataOperationChain doc: dependsUpon) {
            traversalResult |= oc.equals(doc);
            if(traversalResult) break;

            traversalResult |= doc.hasInAllDependents(oc);
            if(traversalResult) break;
        }
        return traversalResult;
    }

    public void addDependency(DataOperationChain dependency) {
        dependsUpon.add(dependency);
    }

    public void addDependent(DataOperationChain dependent) {
        dependents.add(dependent);
    }

    public boolean hasDependents(){
        return !dependents.isEmpty();
    }

    private void updateLevelInMap(int dependencyLevel, int averageRecordsPerDeLevel) {
        synchronized(operationChainsByLevel) {
            if(!operationChainsByLevel.containsKey(dependencyLevel)) {
                operationChainsByLevel.put(dependencyLevel, new ArrayList<>(averageRecordsPerDeLevel));
            }
            operationChainsByLevel.get(getDependencyLevel()).add(this);
        }
    }

    private void removeFromMap(int dependencyLevel) {
        synchronized(operationChainsByLevel) {
            ArrayList<DataOperationChain> targetList = operationChainsByLevel.get(dependencyLevel);
            if(null != targetList) {
                DataOperationChain removedOC = targetList.remove(targetList.size() - 1);
                if(removedOC!=null && removedOC!=this)
                    targetList.set(targetList.indexOf(this), removedOC);
            }
        }
    }

    public void markAllDependencyLevelsDirty() {
        if(isDependencyLevelDirty)
            return;
        isDependencyLevelDirty = true;
        for (DataOperationChain doc: dependents) {
            doc.markAllDependencyLevelsDirty();
        }
    }

    public void updateAllDependencyLevel() {

        int oldLevel = dependencyLevel;
        updateDependencyLevel();

        if(oldLevel!=dependencyLevel) {
            removeFromMap(oldLevel);
            updateLevelInMap(dependencyLevel, averageRecordsPerDeLevel);
            for (DataOperationChain doc: dependents) {
                doc.updateAllDependencyLevel();
            }
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

//    public int currentDependencyLevel() {
//        int dependencyLevel = 0;
//        for(DataOperationChain oc: dependsUpon) {
//            if(oc.getDependencyLevel()>=dependencyLevel) {
//                dependencyLevel = oc.getDependencyLevel()+1;
//            }
//        }
//        return dependencyLevel;
//    }

//    public void pushDependencyLevelToDependents(int newDependencyLevel) {
//
//        if(newDependencyLevel>dependencyLevel) {
//            removeFromMap(dependencyLevel);
//            dependencyLevel = newDependencyLevel;
//            updateLevelInMap(dependencyLevel);
//            for (DataOperationChain doc: dependents) {
//                doc.pushDependencyLevelToDependents(newDependencyLevel+1);
//            }
//        }
//
//    }

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
