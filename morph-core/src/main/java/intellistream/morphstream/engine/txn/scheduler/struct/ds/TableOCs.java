package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import java.util.concurrent.ConcurrentHashMap;

public class TableOCs<Task extends OperationChain> {
    public ConcurrentHashMap<Integer, Holder<Task>> threadOCsMap = new ConcurrentHashMap<>();//ThreadId -> Holder
    public TableOCs(int totalThreads) {
        int i;
        for (i = 0; i < totalThreads; i++) {
            threadOCsMap.put(i, new Holder());
        }
    }
}
