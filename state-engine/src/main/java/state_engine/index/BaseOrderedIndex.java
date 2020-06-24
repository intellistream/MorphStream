package state_engine.index;
import state_engine.storage.TableRecord;
import state_engine.storage.TableRecords;
public abstract class BaseOrderedIndex {
    public abstract TableRecord SearchRecord(String key);
    public abstract void SearchRecords(String secondary_key, TableRecords records);
    public abstract void InsertRecord(String key, TableRecord record);
}
