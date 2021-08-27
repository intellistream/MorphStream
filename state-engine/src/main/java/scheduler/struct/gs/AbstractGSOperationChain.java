package scheduler.struct.gs;

import scheduler.context.AbstractGSTPGContext;
import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public abstract class AbstractGSOperationChain<ExecutionUnit extends GSOperation> extends OperationChain<ExecutionUnit> {
    public AbstractGSTPGContext context = null;
    protected final ConcurrentSkipListMap<AbstractGSOperationChain, ExecutionUnit> ocFdChildren;

    public AbstractGSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
        this.ocFdChildren = new ConcurrentSkipListMap<>();
    }

    public void addOperation(ExecutionUnit op) {
        operations.add(op);
    }

    public <T extends AbstractGSOperationChain> Collection<T> getFDChildren() {
        return (Collection<T>) ocFdChildren.keySet();
    }


    public void setContext(AbstractGSTPGContext context) {
        if (this.context == null) {
            this.context = context;
        }
    }

    @Override
    protected void setupDependency(ExecutionUnit targetOp, OperationChain<ExecutionUnit> parentOC, ExecutionUnit parentOp) {
        this.ocFdParents.putIfAbsent(parentOC, parentOp);
        this.ocFdParentsCount.incrementAndGet();
        // add child for parent OC
        if (parentOC instanceof AbstractGSOperationChain) {
            ((AbstractGSOperationChain) parentOC).ocFdChildren.putIfAbsent(this, targetOp);
        } else {
            throw new UnsupportedOperationException("Wrong operation chain type: " + parentOC);
        }
    }

    public void updateDependency() {
        ocFdParentsCount.decrementAndGet();
    }

    public boolean hasChildren() {
        return !ocFdChildren.isEmpty();
    }

    public void rollbackDependency() {
        ocFdParentsCount.incrementAndGet();
    }
}
