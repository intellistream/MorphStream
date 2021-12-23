package scheduler.oplevel.context;


import scheduler.oplevel.struct.Operation;
import scheduler.oplevel.struct.OperationChain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class OPLayeredContextWithAbort extends OPLayeredContext {
    public int rollbackLevel = -1; // initialized to 0 if thread not required to be rollbacked.
    public boolean isRollbacked = false; // initialized to 0 if thread not required to be rollbacked.

    //The table name is hard-coded.
    public OPLayeredContextWithAbort(int thisThreadId) {
        super(thisThreadId);
    }
}