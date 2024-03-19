package intellistream.morphstream.api.input.statistic;

import lombok.Getter;
import org.apache.commons.lang3.tuple.MutablePair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  OwnershipTable for each key
 */
public class WorkerSideOwnershipTable {
    @Getter
    protected int totalWorker;
    public AtomicBoolean ownershipTableReady = new AtomicBoolean(false);
    public ByteBuffer ownershipTableBuffer;
    protected final ConcurrentHashMap<String, Integer> ownershipTable = new ConcurrentHashMap<>();//Key -> OwnerWorkerId
    @Getter
    protected final List<String> keysForThisWorker = new ArrayList<>();
    public final ConcurrentHashMap<String, int[]> tableNameToValueList = new ConcurrentHashMap<>();
    public WorkerSideOwnershipTable(int totalWorker) {
        this.totalWorker = totalWorker;
    }
    public void putEachKeyForThisWorker(String key) {
        keysForThisWorker.add(key);
    }
    public void initTableNameToValueList(String tableName) {
        tableNameToValueList.put(tableName, new int[keysForThisWorker.size()]);
    }
    public void putEachOwnership(String key, Integer workerId) {
        ownershipTable.put(key, workerId);
    }

    public int getOwnershipWorkerId(String key) {
        return ownershipTable.get(key);
    }
    public boolean isWorkerOwnKey(int workerId, String key) {
        return ownershipTable.get(key) == workerId;
    }
    public void clean() {
        ownershipTable.clear();
        keysForThisWorker.clear();
        tableNameToValueList.clear();
        ownershipTableBuffer = null;
        ownershipTableReady.set(false);
    }
}
