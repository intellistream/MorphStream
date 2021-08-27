package scheduler.struct.gs;

import scheduler.context.AbstractGSTPGContext;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * We still call it OperationChain in TPG but with different representation
 * The OperationChain only tries to maintain a data structure for the ease of temporal dependencies construction.
 */
public class GSOperationChainWithAbort extends AbstractGSOperationChain<GSOperationWithAbort> {
    // maintains descendants of header operation in the oc. HeaderOperation -> Queue<Descendants>
    private final ConcurrentSkipListMap<GSOperationWithAbort, Queue<GSOperationWithAbort>> headerLdDescendants;
    public boolean needAbortHandling = false; // The abort handling in GS should be residing in each operation chain
    public Queue<GSOperationWithAbort> failedOperations = new ArrayDeque<>();


    public GSOperationChainWithAbort(String tableName, String primaryKey) {
        super(tableName, primaryKey);
        this.headerLdDescendants = new ConcurrentSkipListMap<>();
    }

    public void addOperation(GSOperationWithAbort op) {
        operations.add(op);
    }

    @Override
    public Collection<GSOperationChainWithAbort> getFDChildren() {
        return super.getFDChildren();
    }

    public void addDescendant(GSOperationWithAbort header, GSOperationWithAbort descendant) {
        Queue<GSOperationWithAbort> q = headerLdDescendants.computeIfAbsent(header, s -> new ConcurrentLinkedDeque<>());
        q.add(descendant);
    }

    public Queue<GSOperationWithAbort> getDescendants(GSOperationWithAbort descendant) {
        return headerLdDescendants.get(descendant);
    }
}
