package scheduler.context.og;

import scheduler.struct.og.structured.bfs.BFSOperation;
import scheduler.struct.og.structured.bfs.BFSOperationChain;

public class OGBFSContext extends OGSContext<BFSOperation, BFSOperationChain> {

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGBFSContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }

    @Override
    public BFSOperationChain createTask(String tableName, String pKey, long bid) {
        BFSOperationChain oc = new BFSOperationChain(tableName, pKey, bid);
//        operationChains.add(oc);
        return oc;
    }
}
