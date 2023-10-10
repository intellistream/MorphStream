package intellistream.morphstream.engine.txn.storage.table;

import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.TableRecords;

import java.io.Closeable;

public interface ITable extends Iterable<TableRecord>, Closeable {
    /**
     * @param primary_key
     * @return we have to return the d_record here as no pointer passing in Java, contrasting to C/CPP.
     */
    TableRecord SelectKeyRecord(String primary_key);

    void SelectRecords(int idx_id, String secondary_key, TableRecords records);
}
