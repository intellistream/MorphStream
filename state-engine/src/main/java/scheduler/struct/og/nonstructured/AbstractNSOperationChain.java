package scheduler.struct.og.nonstructured;

import scheduler.context.og.AbstractOGNSContext;
import scheduler.struct.og.OperationChain;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public abstract class AbstractNSOperationChain<ExecutionUnit extends NSOperation> extends OperationChain<ExecutionUnit> {
    public AbstractOGNSContext context = null;

    public AbstractNSOperationChain(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }

    public void setContext(AbstractOGNSContext context) {
        if (this.context == null) {
            this.context = context;
        }
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
