package engine.txn.scheduler.struct.recovery;

import java.util.ArrayDeque;

public class Task{
    ArrayDeque<OperationChain> ocs = new ArrayDeque<>();
    public void add(OperationChain oc) {
        ocs.add(oc);
    }
}
