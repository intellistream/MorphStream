package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.HashMap;

public class LocalSVCCDatastore {
    private final String tableName;
    private final HashMap<Integer, Integer> dataStore = new HashMap<>();

    public LocalSVCCDatastore(String tableName) {
        this.tableName = tableName;
        int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
        for (int i = 0; i < NUM_ITEMS; i++) {
            dataStore.put(i, 0);
        }
    }

    public int readSVCCLocalState(int key) {
        return dataStore.get(key);
    }

    public void writeSVCCLocalState(int key, int value) {
        dataStore.put(key, value);
    }
}
