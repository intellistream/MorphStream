package state_engine.storage;
public class SchemaRecords {
    final int max_size_;
    int curr_size_ = 0;
    SchemaRecord[] records_;
    public SchemaRecords(int max_size) {
        max_size_ = max_size;
        records_ = new SchemaRecord[max_size];
    }
    void InsertRecord(SchemaRecord record) {
        assert (curr_size_ < max_size_);
        records_[curr_size_] = record;
        ++curr_size_;
    }
    void Clear() {
        curr_size_ = 0;
    }
    public SchemaRecord getRecords_(int index) {
        return records_[index];
    }
    public void setRecords_(int index, SchemaRecord record) {
        records_[index] = record;
    }
}
