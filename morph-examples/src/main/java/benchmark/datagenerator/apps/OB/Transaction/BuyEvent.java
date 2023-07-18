package benchmark.datagenerator.apps.OB.Transaction;

import benchmark.datagenerator.Event;

/**
 * OnlineBidding Event for (buy)
 * Created by curry on 17/3/22.
 */
public class BuyEvent extends Event {
    private final int id;
    private final int key;
    private final boolean isAbort;

    public BuyEvent(int id, int key, boolean isAbort) {
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
