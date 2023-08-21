package durability.recovery.dependency;

import durability.struct.Logging.DependencyLog;
import scheduler.signal.recovery.OnParentLogUpdateSignal;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class CommandTask implements Comparable<CommandTask> {
    public CSContext context;
    public DependencyLog dependencyLog;
    public ArrayList<CommandTask> children = new ArrayList<>();
    public ArrayList<CommandTask> parents = new ArrayList<>();
    public AtomicInteger parentCount = new AtomicInteger(0);
    private boolean isDependencyLevelCalculated = false;
    public int dependencyLevel = 0;
    public CommandTask(DependencyLog dependencyLog) {
        this.dependencyLog = dependencyLog;
        this.parentCount.set(dependencyLog.getInEdges().size());
    }

    @Override
    public int compareTo(CommandTask o) {
        double thisPriority = Double.parseDouble(this.dependencyLog.id);
        double otherPriority = Double.parseDouble(o.dependencyLog.id);
        return Double.compare(thisPriority, otherPriority);
    }
    public void addChild(CommandTask child) {
        children.add(child);
    }
    public void addParent(CommandTask parent) {
        parents.add(parent);
    }
    public void setContext(CSContext context) {
        this.context = context;
    }
    public void calculateDependencyLevelDuringExploration() {
        dependencyLevel = 0;
        for (CommandTask parent : parents) {
            assert parent.hasValidDependencyLevel();
            dependencyLevel = Math.max(dependencyLevel, parent.dependencyLevel + 1);
        }
    }
    public void layeredNotifyChildren() {
        for (CommandTask child : children) {
            child.context.layerBuildHelperQueue.add(new OnParentLogUpdateSignal(child));
        }
    }
    public synchronized boolean hasValidDependencyLevel() {
        return isDependencyLevelCalculated;
    }
    public void updateDependencies(){
        this.parentCount.decrementAndGet();
        assert this.parentCount.get() >= 0;
    }
    public boolean canCalculateDependencyLevel() {
        return parentCount.get() == 0;
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }
}
