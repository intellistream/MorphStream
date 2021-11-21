package scheduler.struct.gs;

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
    public boolean needAbortHandling = false; // The abort handling in GS should be residing in each operation chain
    public Queue<GSOperationWithAbort> failedOperations = new ArrayDeque<>();


    public GSOperationChainWithAbort(String tableName, String primaryKey, long bid) {
        super(tableName, primaryKey, bid);
    }

    public void addOperation(GSOperationWithAbort op) {
        operations.add(op);
        op.setOC(this); // set OC for op to enable txn abort.
    }

    @Override
    public Collection<GSOperationChainWithAbort> getChildren() {
        return super.getChildren();
    }

    @Override
    public void clear() {
        super.clear();
        needAbortHandling = false;
        failedOperations.clear();
    }
}
