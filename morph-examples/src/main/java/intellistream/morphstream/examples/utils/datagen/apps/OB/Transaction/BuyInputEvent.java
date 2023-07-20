package intellistream.morphstream.examples.utils.datagen.apps.OB.Transaction;

import intellistream.morphstream.examples.utils.datagen.InputEvent;

/**
 * OnlineBidding Event for (buy)
 * Created by curry on 17/3/22.
 */
public class BuyInputEvent extends InputEvent {
    private final int id;
    private final int key;
    private final boolean isAbort;

    public BuyInputEvent(int id, int key, boolean isAbort) {
        this.id = id;
        this.key = key;
        this.isAbort = isAbort;
    }

    @Override
    public String toString() {
        String str = String.valueOf(id) + "," + key +
                "," + isAbort;

        return str;
    }
}
