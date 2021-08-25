package scheduler.context;

import scheduler.struct.dfs.DFSOperation;
import scheduler.struct.dfs.DFSOperationChain;

public class DFSLayeredTPGContext extends LayeredTPGContext<DFSOperation, DFSOperationChain> {

    //TODO: Make it flexible to accept other applications.
    //The table name is hard-coded.
    public DFSLayeredTPGContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }

    @Override
    public DFSOperationChain createTask(String tableName, String pKey) {
        return new DFSOperationChain(tableName, pKey);
    }
}
