package benchmark.datagenerator.apps.GS.TPGTxnGenerator.Transaction;
import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class GSEvent extends Event {
    private final int id;
    private final int[] keys;

    public GSEvent(int id, int[] keys) {
        this.id = id;
        this.keys = keys;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }

        return str.toString();
    }
}
