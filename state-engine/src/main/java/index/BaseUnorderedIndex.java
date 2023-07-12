package index;

import storage.TableRecord;

import java.util.HashMap;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord> {
    protected final int partition_num;
    protected final int num_items;
    protected final int delta;

    protected BaseUnorderedIndex(int partitionNum, int numItems) {
        partition_num = partitionNum;
        num_items = numItems;
        this.delta = num_items / partition_num;
    }
    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

    public abstract TableRecord SearchRecord(String primary_key);

    public abstract boolean InsertRecord(String key, TableRecord record);
    public abstract boolean InsertRecord(String key, TableRecord record, int partition_id);
    public abstract HashMap<String, TableRecord> getTableIndexByPartitionId(int partitionId);
}
