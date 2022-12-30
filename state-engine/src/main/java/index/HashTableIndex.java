package index;

import storage.TableRecord;
import utils.lib.ConcurrentHashMap;

import java.util.HashMap;
import java.util.Iterator;

public class HashTableIndex extends BaseUnorderedIndex {
    private final HashMap<String, TableRecord> hash_index_ = new HashMap<>();
    private final ConcurrentHashMap<String, TableRecord> hash_index_concurrent_ = new ConcurrentHashMap<>(); //Ensure insertion concurrency correctness

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return hash_index_concurrent_.get(primary_key);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        hash_index_concurrent_.put(key, record);
        return true;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return hash_index_concurrent_.values().iterator();
    }

    public Iterator<String> primaryKeyIterator() {
        return hash_index_concurrent_.keySet().iterator();
    }

}
