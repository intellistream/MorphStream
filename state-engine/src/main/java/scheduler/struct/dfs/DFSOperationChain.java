package scheduler.struct.dfs;

import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class DFSOperationChain extends OperationChain<DFSOperation> {
    private final ConcurrentSkipListMap<DFSOperationChain, DFSOperation> ocFdChildren;

    public DFSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
        this.ocFdChildren = new ConcurrentSkipListMap<>();
    }

    public Collection<DFSOperationChain> getFDChildren() {
        return ocFdChildren.keySet();
    }

    @Override
    protected void setupDependency(DFSOperation targetOp, OperationChain<DFSOperation> parentOC, DFSOperation parentOp) {
        this.ocFdParents.putIfAbsent(parentOC, parentOp);
        this.ocFdParentsCount.incrementAndGet();
        parentOp.addChild(targetOp, MetaTypes.DependencyType.FD);
        // add child for parent OC
        if (parentOC instanceof DFSOperationChain) {
            ((DFSOperationChain) parentOC).ocFdChildren.putIfAbsent(this, targetOp);
        } else {
            throw new UnsupportedOperationException("Wrong operation chain type: " + parentOC);
        }
    }

    public void updateDependency() {
        ocFdParentsCount.decrementAndGet();
    }

}
