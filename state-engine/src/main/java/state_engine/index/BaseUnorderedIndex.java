package state_engine.index;

import state_engine.storage.TableRecord;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord> {
    public abstract TableRecord SearchRecord(String primary_key);

    public abstract boolean InsertRecord(String key, TableRecord record);
}
