package engine.txn.storage;

/**
 * A hack ref to SchemaRecord, simulating C++ pointer.
 */
public class SchemaRecordRef {
    public int cnt = 0;
    private volatile SchemaRecord record;
    private String name;

    public boolean isEmpty() {
        return cnt == 0;
    }

    public SchemaRecord getRecord() {
        return record;
    }

    public void setRecord(SchemaRecord record) {
        this.record = record;
        cnt++;
    }

}
