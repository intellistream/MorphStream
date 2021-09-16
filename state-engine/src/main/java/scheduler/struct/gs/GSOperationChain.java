package scheduler.struct.gs;

import java.util.Collection;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class GSOperationChain extends AbstractGSOperationChain<GSOperation> {
    public GSOperationChain(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }

    @Override
    public Collection<GSOperationChain> getChildren() {
        return super.getChildren();
    }
}
