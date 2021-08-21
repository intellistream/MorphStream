package scheduler.struct.bfs;

import scheduler.struct.OperationChain;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class BFSOperationChain extends OperationChain<BFSOperation> {

    public BFSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }
}
