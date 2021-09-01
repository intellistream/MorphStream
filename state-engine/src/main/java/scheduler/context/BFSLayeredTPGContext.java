package scheduler.context;

import scheduler.struct.layered.bfs.BFSOperation;
import scheduler.struct.layered.bfs.BFSOperationChain;

public class BFSLayeredTPGContext extends LayeredTPGContext<BFSOperation, BFSOperationChain> {

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public BFSLayeredTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }

    @Override
    public BFSOperationChain createTask(String tableName, String pKey) {
        return new BFSOperationChain(tableName, pKey);
    }
};
