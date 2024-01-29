package intellistream.morphstream.engine.txn.index;

import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.record.TableRecords;

public abstract class BaseOrderedIndex {
    public abstract TableRecord SearchRecord(String key);

    public abstract void SearchRecords(String secondary_key, TableRecords records);

    public abstract void InsertRecord(String key, TableRecord record);
}
