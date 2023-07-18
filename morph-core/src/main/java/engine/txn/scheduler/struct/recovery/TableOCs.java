package engine.txn.scheduler.struct.recovery;

import util.ConcurrentHashMap;

public class TableOCs {
    public ConcurrentHashMap<Integer, Holder> threadOCsMap = new ConcurrentHashMap<>();
    public TableOCs(Integer num_op, int offset) {
        int i;
        for (i = 0; i < num_op; i++) {
            threadOCsMap.put(i + offset, new Holder());
        }
    }
}
