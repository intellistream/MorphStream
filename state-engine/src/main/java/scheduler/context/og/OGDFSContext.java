package scheduler.context.og;

import scheduler.struct.og.structured.dfs.DFSOperation;
import scheduler.struct.og.structured.dfs.DFSOperationChain;

public class OGDFSContext extends OGSContext<DFSOperation, DFSOperationChain> {

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public OGDFSContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }

    @Override
    public DFSOperationChain createTask(String tableName, String pKey, long bid) {
        DFSOperationChain oc = new DFSOperationChain(tableName, pKey, bid);
//        operationChains.add(oc);
        return oc;
    }
}
