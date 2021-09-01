package scheduler.context;

import scheduler.struct.layered.bfs.BFSOperationChain;

public class BFSLayeredTPGContextWithAbort extends BFSLayeredTPGContext {

    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public BFSLayeredTPGContextWithAbort(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }

    @Override
    public BFSOperationChain createTask(String tableName, String pKey) {
        return new BFSOperationChain(tableName, pKey);
    }
}
