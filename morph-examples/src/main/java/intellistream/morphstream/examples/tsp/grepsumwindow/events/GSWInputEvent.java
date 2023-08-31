package intellistream.morphstream.examples.tsp.grepsumwindow.events;

import intellistream.morphstream.api.input.InputEvent;

/**
 * Streamledger related transaction data
 */
public class GSWInputEvent extends InputEvent {
    private final int id;
    private final int[] keys;
    private final boolean isWindow;

    public GSWInputEvent(int id, int[] keys, boolean isWindow) {
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
