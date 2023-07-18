package engine.txn.index;

import engine.txn.storage.TableRecord;
import engine.txn.storage.TableRecords;

public abstract class BaseOrderedIndex {
    public abstract TableRecord SearchRecord(String key);

    public abstract void SearchRecords(String secondary_key, TableRecords records);

    public abstract void InsertRecord(String key, TableRecord record);
}
