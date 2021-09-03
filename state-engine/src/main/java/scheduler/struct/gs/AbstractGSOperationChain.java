package scheduler.struct.gs;

import scheduler.context.AbstractGSTPGContext;
import scheduler.struct.OperationChain;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public abstract class AbstractGSOperationChain<ExecutionUnit extends GSOperation> extends OperationChain<ExecutionUnit> {
    public AbstractGSTPGContext context = null;

    public AbstractGSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }

    public void addOperation(ExecutionUnit op) {
        operations.add(op);
    }


    public void setContext(AbstractGSTPGContext context) {
        if (this.context == null) {
            this.context = context;
        }
    }

    @Override
    public void clear() {
        super.clear();
        context = null;
    }

    public void updateDependency() {
        ocParentsCount.decrementAndGet();
    }

    public boolean hasChildren() {
        return !ocChildren.isEmpty();
    }

    public void rollbackDependency() {
        ocParentsCount.incrementAndGet();
    }
}
