package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.vnf.LocalSVCCDatastore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationStateManagerStatic {

    private final ReplicationDataStore globalState = new ReplicationDataStore("testTable");
    private final int NUM_ITEMS = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private static final Map<Integer, Boolean> invalidationFlags = new ConcurrentHashMap<>();
    private static final Map<Integer, ReentrantLock> tupleWriteLocks = new ConcurrentHashMap<>();

    public ReplicationStateManagerStatic() {
        for (int i = 0; i < NUM_ITEMS; i++) {
            invalidationFlags.put(i, false);
            tupleWriteLocks.put(i, new ReentrantLock());
        }
    }

    public int readReplicatedState(LocalSVCCDatastore localCache, int key) {
        if (invalidationFlags.getOrDefault(key, true) || !localCache.containsKey(key)) {
            synchronized (this) {
                localCache.writeLocalState(key, globalState.readRepGlobalState(key));
                invalidationFlags.put(key, false); // Reset invalidation flag for this tuple after refresh
            }
        }
        return localCache.readLocalState(key);
    }

    public void writeReplicatedState(int key, int value) {
        ReentrantLock lock = tupleWriteLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            globalState.writeRepGlobalState(key, value);
            invalidationFlags.put(key, true);
        } finally {
            lock.unlock();
        }
    }
}
