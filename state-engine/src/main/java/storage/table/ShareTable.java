package storage.table;

import index.BaseUnorderedIndex;
import index.HashTableIndex;
import index.StdUnorderedIndex;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.TableRecords;
import storage.datatype.DataBox;

import java.util.Iterator;
import java.util.List;

public class ShareTable extends BaseTable {
    //    private final BaseOrderedIndex[] secondary_indexes_;
    private final BaseUnorderedIndex primary_index_;

    public ShareTable(RecordSchema schema, String tableName, boolean is_thread_safe) {
        super(schema, tableName);
        if (is_thread_safe) {
//#if defined(CUCKOO_INDEX)
//			primary_index_ = new CuckooIndex();
//#else
//			primary_index_ = new StdUnorderedIndexMT();
//#endif
            primary_index_ = new HashTableIndex();//here, we decide which index to use.
//            secondary_indexes_ = new BaseOrderedIndex[secondary_count_];
//            for (int i = 0; i < secondary_count_; ++i) {
//                secondary_indexes_[i] = new StdOrderedIndexMT();
//            }
        } else {
            primary_index_ = new StdUnorderedIndex();
//            secondary_indexes_ = new BaseOrderedIndex[secondary_count_];
//            for (int i = 0; i < secondary_count_; ++i) {
//                secondary_indexes_[i] = new StdOrderedIndex();
//            }
        }
    }

    @Override
    public TableRecord SelectKeyRecord(String primary_key) {
        return primary_index_.SearchRecord(primary_key);
    }

    @Override
    public void SelectRecords(int idx_id, String secondary_key, TableRecords records) {
//        secondary_indexes_[idx_id].SearchRecords(secondary_key, records);
        throw new UnsupportedOperationException();
    }

    ///////////////////INSERT//////////////////
    @Override
    public boolean InsertRecord(TableRecord record) {
        SchemaRecord record_ptr = record.record_;
        assert record.record_ != null;
        if (primary_index_.InsertRecord(record_ptr.GetPrimaryKey(), record)) {
            int records = numRecords.getAndIncrement();
            record.setID(new RowID(records));
            //TODO: build secondary index here
//			for (int i = 1; i < secondary_count_; ++i) {
//				secondary_indexes_[i].InsertRecord(record_ptr.GetSecondaryKey(i), record);
//			}
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void clean() {
    }

    @Override
    public SchemaRecord deleteRecord(RowID rid) {
        return null;
    }

    @Override
    public SchemaRecord getRecord(RowID rid) {
        return null;
    }

    @Override
    public SchemaRecord updateRecord(List<DataBox> values, RowID rid) {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return primary_index_.iterator();
    }
}
