package engine.txn.storage;

public class RecordInfo {
    private String tableName;
    private String primaryKey;
    private TableRecord record;

    public RecordInfo(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.record = null;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public TableRecord getRecord() {
        return record;
    }

    public void setRecord(TableRecord record) {
        this.record = record;
    }
}
