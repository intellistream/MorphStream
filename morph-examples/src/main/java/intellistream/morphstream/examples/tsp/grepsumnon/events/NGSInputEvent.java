package intellistream.morphstream.examples.tsp.grepsumnon.events;

import intellistream.morphstream.api.input.InputEvent;

public class NGSInputEvent extends InputEvent {
    private final int id;
    private final int[] keys;
    private final boolean isAbort;
    private final boolean isNon_Deterministic_StateAccess;

    public NGSInputEvent(int id, int[] keys, boolean isAbort, boolean isNonDeterministicStateAccess) {
        this.id = id;
        this.keys = keys;
        this.isAbort = isAbort;
        this.isNon_Deterministic_StateAccess = isNonDeterministicStateAccess;
    }

    @Override
    public String toString() {
        StringBuilder str;
        str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }
        str.append(",").append(isAbort);
        str.append(",").append(isNon_Deterministic_StateAccess);
        return str.toString();
    }
}