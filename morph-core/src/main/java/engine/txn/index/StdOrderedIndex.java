package engine.txn.index;

import engine.txn.storage.TableRecord;
import engine.txn.storage.TableRecords;

import java.util.SortedMap;
import java.util.TreeMap;

public class StdOrderedIndex extends BaseOrderedIndex {
    SortedMap<String, TableRecord> index_ = new TreeMap<>();

    @Override
    public TableRecord SearchRecord(String key) {
        return index_.get(key);
    }

    /**
     * equal_range: Returns the bounds of the subrange that includes all the elements of the range [first,last) with values equivalent to val.
     *
     * @param secondary_key
     * @return
     */
    @Override
    public void SearchRecords(String secondary_key, TableRecords records) {
        final SortedMap<String, TableRecord> sortedMap = index_.tailMap(secondary_key);
        for (TableRecord tableRecord : sortedMap.values()) {
            records.InsertRecord(tableRecord);
        }
    }

    @Override
    public void InsertRecord(String key, TableRecord record) {
        index_.put(key, record);
    }
}
