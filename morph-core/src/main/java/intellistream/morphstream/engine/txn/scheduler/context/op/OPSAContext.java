package intellistream.morphstream.engine.txn.scheduler.context.op;


public class OPSAContext extends OPSContext {
    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public OPSAContext(int thisThreadId) {
        super(thisThreadId);
    }
}