package scheduler.struct.gs;

import scheduler.context.GSTPGContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes;
import scheduler.struct.OperationChain;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class GSOperationChain extends AbstractGSOperationChain<GSOperation> {
    public GSOperationChain(String tableName, String primaryKey) {
        super(tableName, primaryKey);
    }

    @Override
    public Collection<GSOperationChain> getFDChildren() {
        return super.getFDChildren();
    }
}
