package scheduler.struct.dfs;

import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class DFSOperationChain<OP extends DFSOperation, OC extends DFSOperationChain> extends OperationChain<OP, OC> implements Comparable<OC> {
    // OperationChainKey -> OperationChain
    private final ConcurrentSkipListMap<OC, OP> ocFdChildren = new ConcurrentSkipListMap<>();
    public boolean isExecuted = false;

    private boolean isDependencyLevelCalculated = false; // we only do this once before executing all OCs.
    private int dependencyLevel = -1;

    public DFSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }

    public DFSOperationChain() {
    }

    @Override
    public void addOperation(OP op) {
        super.addOperation(op);
        op.setOC(this);
    }

    @Override
    protected void setupDependency(OP targetOp, OC parentOC, OP parentOp) {
        this.getOcFdParents().putIfAbsent(parentOC, parentOp);
        this.getOcFdParentsCount().incrementAndGet();
        parentOp.addChild(targetOp, MetaTypes.DependencyType.FD);
        parentOC.addFDChild(this, targetOp);
    }

    public void addFDChild(OC childOC, OP childOp) {
        this.ocFdChildren.putIfAbsent(childOC, childOp);
    }

    public void updateDependency() {
        getOcFdParentsCount().decrementAndGet();
    }

    public Collection<OC> getFDChildren() {
        return ocFdChildren.keySet();
    }
}
