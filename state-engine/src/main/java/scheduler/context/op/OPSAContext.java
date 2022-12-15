package scheduler.context.op;


import stage.Stage;

public class OPSAContext extends OPSContext {
    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public OPSAContext(int thisThreadId, Stage stage) {
        super(thisThreadId, stage);
    }
}