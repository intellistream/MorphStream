package state_engine.index;
import state_engine.storage.TableRecord;

import java.util.Iterator;
public class StdUnorderedIndex extends BaseUnorderedIndex {
    @Override
    public TableRecord SearchRecord(String primary_key) {
        return null;
    }
    @Override
    public boolean InsertRecord(String s, TableRecord record) {
        return false;
    }
    @Override
    public Iterator<TableRecord> iterator() {
        return null;
    }
}
