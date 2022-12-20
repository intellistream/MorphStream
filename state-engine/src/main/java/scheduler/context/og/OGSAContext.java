package scheduler.context.og;

import stage.Stage;

public class OGSAContext extends OGSContext {

    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public OGSAContext(int thisThreadId, int totalThreads, Stage stage) {
        super(thisThreadId, totalThreads, stage);
    }
}
