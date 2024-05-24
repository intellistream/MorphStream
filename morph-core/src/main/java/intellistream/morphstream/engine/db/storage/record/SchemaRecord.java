package intellistream.morphstream.engine.db.storage.record;

import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import intellistream.morphstream.engine.db.storage.table.RowID;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.utils.Utils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper class for an individual d_record. Simply stores a list of DataBoxes.
 */
public class SchemaRecord {
    private final DataBox single_value;//only used by TSTREAM.
    public boolean is_visible_;
    public RecordSchema schema_ptr_;
    private RowID id;
    @Getter
    private volatile List<DataBox> values;//TODO: Note that, I'm not following the pointer based implementation in Cavalia (C++ based). This may or may not be suitable...

    public SchemaRecord(DataBox values) {
        this.single_value = values;
    }

    public SchemaRecord(List<DataBox> values) {
        this.values = values;
        single_value = null;
    }

    public SchemaRecord(SchemaRecord _record_ptr) {
        this.id = _record_ptr.id;
        this.values = Utils.memcpy(_record_ptr.values);//memcpy( * data, data_ptr_, data_size_);
        single_value = null;
    }

    public DataBox getValue() {
        return this.single_value;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SchemaRecord)) {
            return false;
        }
        SchemaRecord otherRecord = (SchemaRecord) other;
        if (values.size() != otherRecord.values.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!(values.get(i).equals(otherRecord.values.get(i)))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        if (values != null) {
            StringBuilder s = new StringBuilder();
            for (DataBox d : values) {
                s.append(d.toString().trim());
                s.append(", ");
            }
            return s.substring(0, s.length() - 2);
        } else if (single_value != null) {
            return single_value.toString();
        } else
            return getId().toString() + " have no value_list";
    }

    public RowID getId() {
        return id;
    }

    public void setID(RowID ID) {
        this.id = ID;
    }

    /**
     * Assume the primary key is always a String, and is always the first field.
     *
     * @return
     */
    public String GetPrimaryKey() {
        return values.get(0).getString();
    }

    public String GetSecondaryKey(int i) {
        return values.get(i).getString();
    }

    public void clean() {
        values = null;
    }

    public void CopyFrom(SchemaRecord src_record) {
        this.id = src_record.id;
        this.values = new ArrayList<>(src_record.values);
    }

    public void updateValues(List<DataBox> value) {
        this.values = Utils.memcpy(value);
    }
}
