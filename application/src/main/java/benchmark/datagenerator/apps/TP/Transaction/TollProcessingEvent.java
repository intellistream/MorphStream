package benchmark.datagenerator.apps.TP.Transaction;

import benchmark.datagenerator.Event;

/**
 * Toll Processing data
 * Created by curry on 17/3/22.
 */
public class TollProcessingEvent extends Event {
    private final int id;
    private final int segmentId;
    private final boolean isAbort;

    public TollProcessingEvent(int id, int segmentId, boolean isAbort) {
        this.id = id;
        this.segmentId = segmentId;
        this.isAbort = isAbort;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        str.append(",").append(segmentId);
        str.append(",").append(isAbort);

        return str.toString();
    }
}
