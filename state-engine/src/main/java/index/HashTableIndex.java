package index;

import storage.TableRecord;

import java.util.HashMap;
import java.util.Iterator;

public class HashTableIndex extends BaseUnorderedIndex {
    private final HashMap<String, TableRecord> hash_index_ = new HashMap<>();

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return hash_index_.get(primary_key);
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        hash_index_.put(key, record);
        return true;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return hash_index_.values().iterator();
    }
}
