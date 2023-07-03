package benchmark.datagenerator.apps.GSW.TPGTxnGenerator.Transaction;
import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class GSWEvent extends Event {
    private final int id;
    private final int[] keys;
    private final boolean isWindow;

    public GSWEvent(int id, int[] keys, boolean isWindow) {
        this.id = id;
        this.keys = keys;
        this.isWindow = isWindow;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }
        str.append(",").append(isWindow);

        return str.toString();
    }
}
