package scheduler.struct.layered.bfs;

import scheduler.struct.layered.LayeredOperationChain;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class BFSOperationChain extends LayeredOperationChain<BFSOperation> {

    public BFSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }
}
