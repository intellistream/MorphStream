package scheduler.struct.og.structured.dfs;

import scheduler.struct.og.structured.SOperationChain;

import java.util.Collection;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class DFSOperationChain extends SOperationChain<DFSOperation> {

    public DFSOperationChain(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }

    @Override
    public Collection<DFSOperationChain> getChildren() {
        return super.getChildren();
    }

    public void updateDependency() {
        ocParentsCount.decrementAndGet();
    }

    public void rollbackDependency() {
        ocParentsCount.incrementAndGet();
    }
}
