package index;

import storage.TableRecord;

import java.util.Iterator;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord> {
    public abstract TableRecord SearchRecord(String primary_key);

    public abstract boolean InsertRecord(String key, TableRecord record);

    public abstract Iterator<String> primaryKeyIterator();
}
