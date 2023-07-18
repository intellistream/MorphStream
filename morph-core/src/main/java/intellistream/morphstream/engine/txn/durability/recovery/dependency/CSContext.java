package intellistream.morphstream.engine.txn.durability.recovery.dependency;


import intellistream.morphstream.engine.txn.scheduler.signal.recovery.OnParentLogUpdateSignal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CSContext {
    public int threadId;
    public HashMap<Integer, ArrayList<CommandTask>> allocatedLayeredBuckets = new HashMap<>();
    public ConcurrentLinkedQueue<OnParentLogUpdateSignal> layerBuildHelperQueue = new ConcurrentLinkedQueue<>();
    public int currentLevel;
    public int maxLevel;
    public int currentLevelIndex;
    public int totalTaskCount;
    public int scheduledTaskCount;
    public CommandTask readyTask;

    public CSContext(int threadId) {
        this.threadId = threadId;
    }

    public boolean finished() {
        assert scheduledTaskCount <= totalTaskCount && currentLevel <= maxLevel;
        return scheduledTaskCount == totalTaskCount && currentLevel == maxLevel;
    }

    public void reset() {
        allocatedLayeredBuckets.clear();
        currentLevel = 0;
        currentLevelIndex = 0;
        maxLevel = 0;
        totalTaskCount = 0;
        scheduledTaskCount = 0;
        layerBuildHelperQueue.clear();
    }

    public ArrayList<CommandTask> CurrentLayer() {
        return allocatedLayeredBuckets.get(currentLevel);
    }

    public void buildBucketsPerThread(Collection<CommandTask> tasks, Collection<CommandTask> roots) {
        ArrayDeque<CommandTask> processedOps = new ArrayDeque<>();
        for (CommandTask task : roots) {
            updateDependencyLevel(processedOps, task);
        }
        while (processedOps.size() != tasks.size()) {
            OnParentLogUpdateSignal signal = layerBuildHelperQueue.poll();
            while (signal != null) {
                CommandTask task = signal.commandTask;
                task.updateDependencies();
                if (task.canCalculateDependencyLevel()) {
                    updateDependencyLevel(processedOps, task);
                }
                signal = layerBuildHelperQueue.poll();
            }
        }

        int localMaxLevel = 0;
        int dependencyLevel;
        for (CommandTask task : tasks) {
            assert task.hasValidDependencyLevel();
            dependencyLevel = task.getDependencyLevel();
            if (localMaxLevel < dependencyLevel) {
                localMaxLevel = dependencyLevel;
            }
            if (!allocatedLayeredBuckets.containsKey(dependencyLevel))
                allocatedLayeredBuckets.put(dependencyLevel, new ArrayList<>());
            allocatedLayeredBuckets.get(dependencyLevel).add(task);
        }
        this.maxLevel = localMaxLevel;
    }

    private void updateDependencyLevel(ArrayDeque<CommandTask> processedOps, CommandTask task) {
        task.calculateDependencyLevelDuringExploration();
        task.layeredNotifyChildren();
        processedOps.add(task);
    }

}
