package storage;

public class TableRecords {
    final int max_size_;
    public int curr_size_ = 0;
    public TableRecord[] records_;

    public TableRecords(int max_size) {
        max_size_ = max_size;
        records_ = new TableRecord[max_size];
    }

    public void InsertRecord(TableRecord record) {
        assert (curr_size_ < max_size_);
        records_[curr_size_] = record;
        ++curr_size_;
    }

    public void Clear() {
        curr_size_ = 0;
    }
}
