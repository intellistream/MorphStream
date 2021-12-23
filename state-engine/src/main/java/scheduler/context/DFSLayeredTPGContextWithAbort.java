package scheduler.context;

import scheduler.struct.layered.dfs.DFSOperationChain;

public class DFSLayeredTPGContextWithAbort extends DFSLayeredTPGContext {

    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public DFSLayeredTPGContextWithAbort(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }
}
