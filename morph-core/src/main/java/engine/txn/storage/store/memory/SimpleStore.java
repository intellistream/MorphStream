package engine.txn.storage.store.memory;

import engine.txn.storage.table.RowID;
import engine.txn.storage.SchemaRecord;
import engine.txn.storage.store.Store;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a simple interface for a table to Store and retrieve SchemaRecord data.
 * TODO: on and off heap implementation in future. Now just use the simple on-heap one.
 */
public class SimpleStore implements Store {
    /**
     * TODO: now, just consecutively store the rows -> use linked-list, this is probably a bad design, fix it in future.
     * TODO: ying jun's design is store in a index. Some argues that index may not be helpful. Let's see...
     * TODO: We could store the rows in a tree like structure based on its primary key!
     *
     * @Update: use concurrent hashmap, but HashMap has poor support for range query, let's see...
     */
    ConcurrentHashMap<Integer, SchemaRecord> rows = new ConcurrentHashMap<>();

    public void clean() {
    }

    public void addrow(RowID rowID, SchemaRecord r) {
        rows.put(rowID.getID(), r);
    }

    public SchemaRecord deleterow(RowID rid) {
        return rows.remove(rid.getID());
    }

    public SchemaRecord getrow(RowID rid) {
        return rows.get(rid.getID());
    }

    public SchemaRecord updaterow(SchemaRecord record, RowID rid) {
        return rows.replace(rid.getID(), record);
    }

    public Iterator<SchemaRecord> getRowIterator() {
        return rows.values().iterator();
    }
}
