package engine.txn.index;

import engine.txn.storage.TableRecord;

import java.util.HashMap;
import java.util.Iterator;

public class StdUnorderedIndex extends BaseUnorderedIndex {
    public StdUnorderedIndex(int partition_num, int num_items) {
        super(partition_num, num_items);
    }

    @Override
    public TableRecord SearchRecord(String primary_key) {
        return null;
    }

    @Override
    public boolean InsertRecord(String s, TableRecord record, int partition_id) {
        return false;
    }

    @Override
    public boolean InsertRecord(String key, TableRecord record) {
        return false;
    }

    @Override
    public HashMap<String, TableRecord> getTableIndexByPartitionId(int partitionId) {
        return null;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return null;
    }
}
