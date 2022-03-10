package scheduler.struct.og.nonstructured;

import java.util.Collection;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class NSOperationChain extends AbstractNSOperationChain<NSOperation> {
    public NSOperationChain(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }

    @Override
    public Collection<NSOperationChain> getChildren() {
        return super.getChildren();
    }
}
