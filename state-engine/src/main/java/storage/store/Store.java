package storage.store;

import storage.SchemaRecord;
import storage.table.RowID;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * This is the interface for a table to Store and retrieve SchemaRecord data.
 * TODO: on and off heap implementation in future. Now just use the simple on-heap one.
 */
public interface Store {
    //TODO: now, just consecutively store the rows -> use linked-list, this is probably a bad design, fix it in future.
    //TODO: ying jun's design is store in a index. Some argues that index may not be helpful. Let's see...
    //TODO: We could store the rows in a tree like structure based on its primary key!
    //TODO: HashMap has poor support for range query.
    HashMap<RowID, SchemaRecord> rows = new LinkedHashMap<>();

    void clean();

    void addrow(RowID rowID, SchemaRecord r);

    SchemaRecord deleterow(RowID rid);

    SchemaRecord getrow(RowID rid);

    SchemaRecord updaterow(SchemaRecord record, RowID rid);

    Iterator<SchemaRecord> getRowIterator();
}
