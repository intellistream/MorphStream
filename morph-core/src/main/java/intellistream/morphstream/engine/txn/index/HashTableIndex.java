package intellistream.morphstream.engine.txn.index;

import intellistream.morphstream.engine.txn.storage.TableRecord;
import java.util.concurrent.ConcurrentHashMap;

import java.util.HashMap;
import java.util.Iterator;

public class HashTableIndex extends BaseUnorderedIndex {
    /**
     * To snapshot and recover in parallel, we need separate hashmap for each partition
     * To support recover database in parallel, we need concurrent hashmap
     */
    private final ConcurrentHashMap<Integer, HashMap<String, TableRecord>> hash_index_by_partition = new ConcurrentHashMap<>();

    public HashTableIndex(int partition_num, int num_items) {
        super(partition_num, num_items);
        for (int i = 0; i < partition_num; i++) {
            hash_index_by_partition.put(i, new HashMap<>());
        }
    }

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return hash_index_by_partition.get(getPartitionId(primary_key)).get(primary_key);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record, int partition_id) {
        hash_index_by_partition.get(partition_id).put(key, record);
        return true;
    }

    @Override
    public HashMap<String, TableRecord> getTableIndexByPartitionId(int partitionId) {
        return this.hash_index_by_partition.get(partitionId);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        hash_index_by_partition.get(getPartitionId(key)).put(key, record);
        return true;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        HashMap<String, TableRecord> temp = new HashMap<>();
        for (int i = 0; i < this.partition_num; i++) {
            temp.putAll(hash_index_by_partition.get(i));
        }
        return temp.values().iterator();
    }
}
