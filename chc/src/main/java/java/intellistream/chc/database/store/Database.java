package java.intellistream.chc.database.store;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Database
 */
public class Database {
    private final ConcurrentHashMap<Key, Object> store;

    public Database() {
        this.store = new ConcurrentHashMap<>();
    }

    /**
     * Find the shared state by the vertex id and the object key
     * @param vertexId vertex id
     * @param objKey object key
     * @return shared state object
     */
    public Object findSharedState(int vertexId, int objKey) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        return this.get(key);
    }

    /**
     * Find the exclusive state by the vertex id, the instance id and the object key
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @return exclusive state object
     */
    public Object findExclusiveState(int vertexId, int instanceId, int objKey) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        return this.get(key);
    }

    /**
     * Put the shared state into the database
     * @param vertexId vertex id
     * @param objKey object key
     * @param value shared state object
     */
    public void putSharedState(int vertexId, int objKey, Object value) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        this.put(key, value);
    }

    /**
     * Put the exclusive state into the database
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @param value exclusive state object
     */
    public void putExclusiveState(int vertexId, int instanceId, int objKey, Object value) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        this.put(key, value);
    }

    /**
     * Remove the shared state from the database
     * @param vertexId vertex id
     * @param objKey object key
     */
    public void removeSharedState(int vertexId, int objKey) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        this.remove(key);
    }

    /**
     * Remove the exclusive state from the database
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     */
    public void removeExclusiveState(int vertexId, int instanceId, int objKey) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        this.remove(key);
    }

    /**
     * Check if the shared state exists in the database
     * @param vertexId vertex id
     * @param objKey object key
     * @return true if the shared state exists, false otherwise
     */
    public boolean containsSharedState(int vertexId, int objKey) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        return this.containsKey(key);
    }

    /**
     * Check if the exclusive state exists in the database
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @return true if the exclusive state exists, false otherwise
     */
    public boolean containsExclusiveState(int vertexId, int instanceId, int objKey) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        return this.containsKey(key);
    }

    private void put(Key key, Object value) {
        this.store.put(key, value);
    }

    private Object get(Key key) {
        return this.store.get(key);
    }

    private void remove(Key key) {
        this.store.remove(key);
    }

    private boolean containsKey(Key key) {
        return this.store.containsKey(key);
    }

    /**
     * Key of the object in the database
     */
    @Data
    private static class Key {
        public final Integer vertexId;
        public final Integer instanceId;
        public final Integer objKey;
        public final StateType stateType;

        public Key(Integer vertexId, Integer instanceId, Integer objKey, StateType stateType) {
            this.vertexId = vertexId;
            this.instanceId = instanceId;
            this.objKey = objKey;
            this.stateType = stateType;
        }
    }

    /**
     * Type of the object's state in the database
     */
    private enum StateType {
        SHARED,
        EXCLUSIVE,
    }
}
