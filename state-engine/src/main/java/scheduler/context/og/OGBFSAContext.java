package scheduler.context.og;

public class OGBFSAContext extends OGBFSContext {

    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public OGBFSAContext(int thisThreadId, int totalThreads) {
        super(thisThreadId, totalThreads);
    }
}
