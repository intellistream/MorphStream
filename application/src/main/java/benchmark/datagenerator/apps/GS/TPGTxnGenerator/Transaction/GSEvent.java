package benchmark.datagenerator.apps.GS.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class GSEvent extends Event {
    private final int id;
    private final int[] keys;
    private final boolean isAbort;

    public GSEvent(int id, int[] keys, boolean isAbort) {
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
