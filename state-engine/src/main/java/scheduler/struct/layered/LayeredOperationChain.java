package scheduler.struct.layered;

import scheduler.struct.AbstractOperation;
import scheduler.struct.OperationChain;

import java.util.Collection;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public abstract class LayeredOperationChain<ExecutionUnit extends AbstractOperation> extends OperationChain<ExecutionUnit> {

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public LayeredOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }

    @Override
    public Collection<LayeredOperationChain> getFDParents() {
        return super.getFDParents();
    }

    public synchronized boolean hasValidDependencyLevel() {
        return isDependencyLevelCalculated;
    }

    public int getDependencyLevel() {
        return dependencyLevel;
    }

    public synchronized void updateDependencyLevel() {
        if (isDependencyLevelCalculated)
            return;
        dependencyLevel = 0;
        for (LayeredOperationChain parent : getFDParents()) {
            if (!parent.hasValidDependencyLevel()) {
                parent.updateDependencyLevel();
            }

            if (parent.getDependencyLevel() >= dependencyLevel) {
                dependencyLevel = parent.getDependencyLevel() + 1;
            }
        }
        isDependencyLevelCalculated = true;
    }
}
