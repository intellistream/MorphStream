package scheduler.struct.gs;

import org.apache.hadoop.util.hash.Hash;
import scheduler.context.GSTPGContext;
import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;
import scheduler.struct.bfs.BFSOperation;
import scheduler.struct.dfs.DFSOperation;
import scheduler.struct.dfs.DFSOperationChain;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class GSOperationChain extends OperationChain<GSOperation> {
    public GSTPGContext context = null;
    private final ConcurrentSkipListMap<GSOperationChain, GSOperation> ocFdChildren;
    public boolean needAbortHandling = false; // The abort handling in GS should be residing in each operation chain
    public Queue<GSOperation> failedOperations = new ArrayDeque<>();


    public GSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
        this.ocFdChildren = new ConcurrentSkipListMap<>();
    }

    public void addOperation(GSOperation op) {
        operations.add(op);
        setContext(op.context);
    }

    public Collection<GSOperationChain> getFDChildren() {
        return ocFdChildren.keySet();
    }

    public Collection<OperationChain<GSOperation>> getFDParents() {
        return ocFdParents.keySet();
    }


    private void setContext(GSTPGContext context) {
        if (this.context == null) {
            this.context = context;
        }
    }

    @Override
    protected void setupDependency(GSOperation targetOp, OperationChain<GSOperation> parentOC, GSOperation parentOp) {
        this.ocFdParents.putIfAbsent(parentOC, parentOp);
        this.ocFdParentsCount.incrementAndGet();
        parentOp.addChild(targetOp, MetaTypes.DependencyType.FD);
        // add child for parent OC
        if (parentOC instanceof GSOperationChain) {
            ((GSOperationChain) parentOC).ocFdChildren.putIfAbsent(this, targetOp);
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
