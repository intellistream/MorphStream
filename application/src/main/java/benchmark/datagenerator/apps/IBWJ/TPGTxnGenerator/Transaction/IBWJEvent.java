package benchmark.datagenerator.apps.IBWJ.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class IBWJEvent extends Event {
    private final int id;
    private final int[] keys;
    private final boolean isAbort;

    public IBWJEvent(int id, int[] keys, boolean isAbort) {
        this.id = id;
        this.keys = keys;
        this.isAbort = isAbort;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }
        str.append(",").append(isAbort);

        return str.toString();
    }
}
