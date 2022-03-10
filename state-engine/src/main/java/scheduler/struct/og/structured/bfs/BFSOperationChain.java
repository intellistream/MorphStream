package scheduler.struct.og.structured.bfs;

import scheduler.struct.og.structured.SOperationChain;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class BFSOperationChain extends SOperationChain<BFSOperation> {

    public BFSOperationChain(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }
}
