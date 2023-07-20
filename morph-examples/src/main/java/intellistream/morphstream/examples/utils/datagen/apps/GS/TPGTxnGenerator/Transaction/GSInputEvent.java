package intellistream.morphstream.examples.utils.datagen.apps.GS.TPGTxnGenerator.Transaction;

import intellistream.morphstream.examples.utils.datagen.InputEvent;

/**
 * Streamledger related transaction data
 */
public class GSInputEvent extends InputEvent {
    private final int id;
    private final int[] keys;
    private final boolean isAbort;

    public GSInputEvent(int id, int[] keys, boolean isAbort) {
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
