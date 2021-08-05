package benchmark.datagenerator.apps.SL.OCScheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * maintains the actual OCs will be constructed based on the data generator.
 */
public class SLDataOperationChain {

    private final String stateId;
    private final ArrayList<SLDataOperationChain> parent = new ArrayList<>();
    private final ArrayList<SLDataOperationChain> children = new ArrayList<>();
    private final HashMap<Integer, ArrayList<SLDataOperationChain>> operationChainsByLevel;
    private final int averageRecordsPerDeLevel;
    private int operationsCount = 0;
    private int dependencyLevel;
    private boolean readForTraversal = false;

    public SLDataOperationChain(String stateId, int averageRecordsPerDeLevel, HashMap<Integer, ArrayList<SLDataOperationChain>> operationChainsByLevel) {
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

    public void registerAllDependenciesToList(ArrayList<String> allDependencies) {
        if (!readForTraversal)
            return;
        readForTraversal = false;
        for (SLDataOperationChain oc : parent) {
            String dependencyName = String.format("%s,%s", stateId, oc.getStateId());
            if (!allDependencies.contains(dependencyName))
                allDependencies.add(dependencyName);
            oc.registerAllDependenciesToList(allDependencies);
        }
    }

    public ArrayList<String> getDependencyChainInfo() {
        ArrayList<String> chains = new ArrayList<>();
        for (SLDataOperationChain oc : parent) {
            ArrayList<String> dependsUponChains = oc.getDependencyChainInfo();
            if (dependsUponChains.size() == 0)
                chains.add(stateId + "->" + oc.getStateId());
            else
                for (String dependsUponChain : dependsUponChains)
                    chains.add(stateId + "->" + dependsUponChain);
        }
        return chains;
    }

    public void markReadyForTraversal() {
        readForTraversal = true;
    }

    public boolean hasInAllDependents(SLDataOperationChain oc) {
        if (!readForTraversal)
            return false;
        readForTraversal = false;

        boolean traversalResult = false;
        for (SLDataOperationChain doc : children) {
            traversalResult = oc.equals(doc);
            if (traversalResult) break;

            traversalResult = doc.hasInAllDependents(oc);
            if (traversalResult) break;
        }
        return traversalResult;
    }

    public boolean isDependUpon(SLDataOperationChain oc) {
        boolean traversalResult = false;
        for (SLDataOperationChain doc : parent) {
            traversalResult = oc.equals(doc);
            if (traversalResult) break;

            // TODO: this method is never used by far, and it is also meaningless
            traversalResult = doc.hasInAllDependents(oc);
            if (traversalResult) break;
        }
        return traversalResult;
    }

    public void addParent(SLDataOperationChain parent) {
        this.parent.add(parent);
    }

    public void addChildren(SLDataOperationChain child) {
        children.add(child);
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }

    private void updateLevelInMap(int dependencyLevel, int averageRecordsPerDeLevel) {
        synchronized (operationChainsByLevel) {
            if (!operationChainsByLevel.containsKey(dependencyLevel)) {
                operationChainsByLevel.put(dependencyLevel, new ArrayList<>(averageRecordsPerDeLevel));
            }
            operationChainsByLevel.get(getDependencyLevel()).add(this);
        }
    }

    private void removeFromMap(int dependencyLevel) {
        synchronized (operationChainsByLevel) {
            ArrayList<SLDataOperationChain> targetList = operationChainsByLevel.get(dependencyLevel);
            if (null != targetList) {
                SLDataOperationChain removedOC = targetList.remove(targetList.size() - 1);
                if (removedOC != null && removedOC != this)
                    targetList.set(targetList.indexOf(this), removedOC);
            }
        }
    }

    public void updateAllDependencyLevel() {

        int oldLevel = dependencyLevel;
        updateDependencyLevel();

        if (oldLevel != dependencyLevel) {
            removeFromMap(oldLevel);
            updateLevelInMap(dependencyLevel, averageRecordsPerDeLevel);
            for (SLDataOperationChain doc : children) {
                doc.updateAllDependencyLevel();
            }
        }

    }

    private void updateDependencyLevel() {
        dependencyLevel = 0;
        for (SLDataOperationChain oc : parent) {
            if (oc.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = oc.getDependencyLevel() + 1;
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
        SLDataOperationChain that = (SLDataOperationChain) o;
        return Objects.equals(stateId, that.stateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateId);
    }
}
