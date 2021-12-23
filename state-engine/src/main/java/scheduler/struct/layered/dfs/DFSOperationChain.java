package scheduler.struct.layered.dfs;

import scheduler.struct.OperationChain;
import scheduler.struct.layered.LayeredOperationChain;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The operationchain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class DFSOperationChain extends LayeredOperationChain<DFSOperation> {

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
