package intellistream.morphstream.engine.txn.index;

import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.TableRecords;
import intellistream.morphstream.engine.txn.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.txn.storage.table.RowID;

import java.util.Iterator;

/**
 * TODO: To be implemented..
 */
public class BtreeIndex extends BaseOrderedIndex {
    public BtreeIndex(IntDataBox intDataBox) {
    }

    public void insertKey(IntDataBox intDataBox, RowID rowID) {
    }

    public Iterator<RowID> sortedScan() {
        return null;
    }

    public int getNumNodes() {
        return 0;
    }

    @Override
    public TableRecord SearchRecord(String key) {
        return null;
    }

    @Override
    public void SearchRecords(String secondary_key, TableRecords records) {
    }

    @Override
    public void InsertRecord(String key, TableRecord record) {
    }
}
