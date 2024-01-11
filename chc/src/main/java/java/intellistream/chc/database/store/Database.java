package java.intellistream.chc.database.store;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Database class
 */
public class Database {
    private final ConcurrentHashMap<Key, Object> store;

    public Database() {
        this.store = new ConcurrentHashMap<>();
    }

    public void put(Key key, Object value) {
        this.store.put(key, value);
    }

    public Object get(Key key) {
        return this.store.get(key);
    }

    public void remove(Key key) {
        this.store.remove(key);
    }

    public boolean containsKey(Key key) {
        return this.store.containsKey(key);
    }

    /**
     * Key of the object in the database
     */
    @Data
    public static class Key {
        public final int vertexId;
        public final int instanceId;
        public final int objKey;
        public final StateType stateType;

        public Key(int vertexId, int instanceId, int objKey, StateType stateType) {
            this.vertexId = vertexId;
            this.instanceId = instanceId;
            this.objKey = objKey;
            this.stateType = stateType;
        }
    }

    /**
     * Type of the object's state in the database
     */
    public enum StateType {
        SHARED,
        EXCLUSIVE,
    }
}
