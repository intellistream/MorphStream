package intellistream.morphstream.examples.utils.datagen.apps.OB.Transaction;

import intellistream.morphstream.examples.utils.datagen.InputEvent;

/**
 * OnlineBidding Event for (alert and top)
 * Created by curry on 17/3/22.
 */
public class OBInputEvent extends InputEvent {
    private final int id;
    private final int[] keys;
    private final boolean isAbort;
    private final int isAlert;

    public OBInputEvent(int id, int[] keys, boolean isAbort, int isAlert) {
        this.id = id;
        this.keys = keys;
        this.isAbort = isAbort;
        this.isAlert = isAlert;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }
        str.append(",").append(isAbort);
        str.append(",").append(isAlert);

        return str.toString();
    }
}
