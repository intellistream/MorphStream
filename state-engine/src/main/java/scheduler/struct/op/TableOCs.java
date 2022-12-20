package scheduler.struct.op;

import utils.lib.ConcurrentHashMap;

//     DD: We basically keep multiple holders and distribute operations among them.
//     For example, holder with key 1 can hold operations on tuples with key, 1-100,
//     holder with key 2 can hold operations on tuples with key 101-200 and so on...
public class TableOCs {
    public ConcurrentHashMap<Integer, Holder> threadOCsMap = new ConcurrentHashMap<>();//each op has a holder.

    public TableOCs(Integer num_op, int offset) {
        int i;
        for (i = 0; i < num_op; i++) {
            threadOCsMap.put(i + offset, new Holder());
        }
    }
}
