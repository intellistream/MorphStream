package java.intellistream.chc.database.store;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Database
 */
public class Database {
    private final ConcurrentHashMap<Key, Record> store;

    public Database() {
        this.store = new ConcurrentHashMap<>();
    }

    /**
     * Find the shared state by the vertex id and the object key
     * @param vertexId vertex id
     * @param objKey object key
     * @return shared state object
     */
    public int findSharedState(int vertexId, int objKey) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        return this.get(key).getValue();
    }

    /**
     * Find the exclusive state by the vertex id, the instance id and the object key
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @return exclusive state object
     */
    public int findExclusiveState(int vertexId, int instanceId, int objKey) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        return this.get(key).getValue();
    }

    /**
     * Put the shared state into the database
     * @param vertexId vertex id
     * @param objKey object key
     * @param value shared state object
     */
    public void putSharedState(int vertexId, int objKey, int value) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        this.put(key, new Record(value, Thread.currentThread().getId()));
    }

    /**
     * Put the exclusive state into the database
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @param value exclusive state object
     */
    public void putExclusiveState(int vertexId, int instanceId, int objKey, int value) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        this.put(key, new Record(value, Thread.currentThread().getId()));
    }

    /**
     * Transfer the exclusive state from one instance to another
     * @param fromVertexId original vertex id
     * @param fromInstanceId original instance id
     * @param fromObjKey original object key
     * @param toVertexId to vertex id
     * @param toInstanceId to instance id
     * @param toObjKey to object key
     */
    public void transferExclusiveState(int fromVertexId, int fromInstanceId, int fromObjKey, int toVertexId, int toInstanceId, int toObjKey) {
        this.putExclusiveState(toVertexId, toInstanceId, toObjKey, this.findExclusiveState(fromVertexId, fromInstanceId, fromObjKey));
        this.removeExclusiveState(fromVertexId, fromInstanceId, fromObjKey);
    }

    /**
     * Transfer the shared state from one vertex to another
     * @param fromVertexId original vertex id
     * @param fromObjKey original object key
     * @param toVertexId to vertex id
     * @param toObjKey to object key
     */
    public void transferExclusiveState(int fromVertexId, int fromObjKey, int toVertexId, int toObjKey) {
        this.putSharedState(toVertexId, toObjKey, this.findSharedState(fromVertexId, fromObjKey));
        this.removeSharedState(fromVertexId, fromObjKey);
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

    /**
     * Get the thread id of the thread executing an exclusive state
     * @param vertexId vertex id
     * @param instanceId instance id
     * @param objKey object key
     * @return thread id
     */
    public long getExecuteThreadId(int vertexId, int instanceId, int objKey) {
        Key key = new Key(vertexId, instanceId, objKey, StateType.EXCLUSIVE);
        if (this.get(key) == null) {
            return -1;  // the state is not in the database
        }
        return this.get(key).getThreadId();
    }

    /**
     * Get the thread id of the thread executing a shared state
     * @param vertexId vertex id
     * @param objKey object key
     * @return thread id
     */
    public long getExecuteThreadId(int vertexId, int objKey) {
        Key key = new Key(vertexId, null, objKey, StateType.SHARED);
        if (this.get(key) == null) {
            return -1;  // the state is not in the database
        }
        return this.get(key).getThreadId();
    }

    private void put(Key key, Record record) {
        this.store.put(key, record);
    }

    private Record get(Key key) {
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
    @AllArgsConstructor
    private static class Key {
        public final Integer vertexId;
        public final Integer instanceId;
        public final Integer objKey;
        public final StateType stateType;
    }

    /**
     * Record in the database
     */
    @Data
    @AllArgsConstructor
    public static class Record {
        private int value;
        private long threadId;
    }

    /**
     * Type of the object's state in the database
     */
    private enum StateType {
        SHARED,
        EXCLUSIVE,
    }
}
