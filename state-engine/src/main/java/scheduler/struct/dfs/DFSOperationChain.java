package scheduler.struct.dfs;

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

    public void updateDependency() {
        ocFdParentsCount.decrementAndGet();
    }

}
