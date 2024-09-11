package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.HashMap;

public class ReplicationDataStore {
    private final String tableName;
    private final HashMap<Integer, Integer> dataStore = new HashMap<>();

    public ReplicationDataStore(String tableName) {
        this.tableName = tableName;
        int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
        for (int i = 0; i < NUM_ITEMS; i++) {
            dataStore.put(i, 0);
        }
    }

    public int readRepGlobalState(int key) {
        return dataStore.get(key);
    }

    public void writeRepGlobalState(int key, int value) {
        dataStore.put(key, value);
    }
}
