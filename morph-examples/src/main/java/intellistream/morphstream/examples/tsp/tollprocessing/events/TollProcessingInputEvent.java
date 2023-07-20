package intellistream.morphstream.examples.tsp.tollprocessing.events;

import intellistream.morphstream.api.InputEvent;

/**
 * Toll Processing data
 * Created by curry on 17/3/22.
 */
public class TollProcessingInputEvent extends InputEvent {
    private final int id;
    private final int segmentId;
    private final boolean isAbort;

    public TollProcessingInputEvent(int id, int segmentId, boolean isAbort) {
        this.id = id;
        this.segmentId = segmentId;
        this.isAbort = isAbort;
    }

    @Override
    public String toString() {
        String str = String.valueOf(id) + "," + segmentId +
                "," + isAbort;

        return str;
    }
}
