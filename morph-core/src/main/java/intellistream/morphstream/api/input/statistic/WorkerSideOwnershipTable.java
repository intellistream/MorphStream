package intellistream.morphstream.api.input.statistic;

import lombok.Getter;
import org.apache.commons.lang3.tuple.MutablePair;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  OwnershipTable for each key
 */
public class WorkerSideOwnershipTable {
    @Getter
    protected int totalWorker;
    public ByteBuffer ownershipTableBuffer;
    public ConcurrentHashMap<Integer, Integer> workerIdToTotalKeys = new ConcurrentHashMap<>();//workerId -> totalKeys
    protected final ConcurrentHashMap<String, Tuple2<Integer, Integer>> ownershipTable = new ConcurrentHashMap<>();//Key -> <OwnerWorkerId, index>
    @Getter
    protected final List<String> keysForThisWorker = new ArrayList<>();
    public String[] valueList;
    @Getter
    public AtomicInteger totalKeys = new AtomicInteger(0);
    public WorkerSideOwnershipTable(int totalWorker) {
        this.totalWorker = totalWorker;
    }
    public void putEachKeyForThisWorker(String key) {
        keysForThisWorker.add(key);
    }
    public void putTotalKeysForWorker(int workerId, int totalKeys) {
        workerIdToTotalKeys.put(workerId, totalKeys);
    }
    public void initValueList() {
        valueList = new String[keysForThisWorker.size()];
    }
    public void putEachOwnership(String key, int workerId, int index) {
        ownershipTable.put(key, new Tuple2<>(workerId, index));
    }

    public int getOwnershipWorkerId(String key) {
        return ownershipTable.get(key)._1;
    }
    public int getOwnershipIndex(String key) { return ownershipTable.get(key)._2; }
    public boolean isWorkerOwnKey(int workerId, String key) {
        return ownershipTable.get(key)._1 == workerId;
    }
    public boolean isFinishLoadValue() {
        return totalKeys.get() == keysForThisWorker.size();
    }
    public void clean() {
        ownershipTable.clear();
        keysForThisWorker.clear();
        valueList = null;
        ownershipTableBuffer = null;
        totalKeys.set(0);
    }
}
