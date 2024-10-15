package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.HashMap;

public class LocalDataStore {
    private final String tableName;
    private final HashMap<Integer, Integer> dataStore = new HashMap<>();

    public LocalDataStore(String tableName) {
        this.tableName = tableName;
        int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
        for (int i = 0; i < NUM_ITEMS; i++) {
            dataStore.put(i, 0);
        }
    }

    public int readLocalState(int key) {
        return dataStore.get(key);
    }

    public void writeLocalState(int key, int value) {
        dataStore.put(key, value);
    }

    public boolean containsKey(int key) {
        return dataStore.containsKey(key);
    }
}
