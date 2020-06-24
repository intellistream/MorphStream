package state_engine.storage.table;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * The RecordSchema of a particular table.
 * <p>
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a d_record conforming to this schema
 */
public class RecordSchema {
    private final int secondary_num_;
    private List<String> fields;
    private List<DataBox> fieldTypes;
    private int size;
    public RecordSchema(List<String> fields, List<DataBox> fieldTypes) {
        assert (fields.size() == fieldTypes.size());
        this.fields = fields;
        this.fieldTypes = fieldTypes;
        this.size = 0;
        for (DataBox dt : fieldTypes) {
            this.size += dt.getSize();
        }
        secondary_num_ = fields.size();
    }
    public int getSecondary_num_() {
        return secondary_num_;
    }
    /**
     * Verifies that a list of DataBoxes corresponds to this schema. A list of
     * DataBoxes corresponds to this schema if the number of DataBoxes in the
     * list equals the number of columns in this schema, and if each DataBox has
     * the same type and size as the columns in this schema.
     *
     * @param values the list of values to check
     * @return a new SchemaRecord with the DataBoxes specified
     * @throws SchemaException if the values specified don't conform to this RecordSchema
     */
    public SchemaRecord verify(List<DataBox> values) throws SchemaException {
        if (values.size() != this.fieldTypes.size()) {
            throw new SchemaException("Different numbers of fields specified.");
        }
        for (int i = 0; i < values.size(); i++) {
            DataBox valueType = values.get(i);
            DataBox fieldType = this.fieldTypes.get(i);
            if (!(valueType.type().equals(fieldType.type()))) {
                throw new SchemaException("Field " + i + " is " + valueType.type() + " instead of " + fieldType.type() + ".");
            }
            if (valueType.getSize() != fieldType.getSize()) {
                throw new SchemaException("Field " + i + " is " + valueType.getSize() + " bytes instead of " + fieldType.getSize() + " bytes.");
            }
        }
        return new SchemaRecord(values);
    }
    /**
     * Serializes the provided d_record into a byte[]. Uses the DataBoxes'
     * serialization methods. A serialized d_record is represented as the
     * concatenation of each serialized DataBox. This method assumes that the
     * input d_record corresponds to this schema.
     *
     * @param record the d_record to encode
     * @return the encoded d_record as a byte[]
     */
    public byte[] encode(SchemaRecord record) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.size);
        for (DataBox value : record.getValues()) {
            byteBuffer.put(value.getBytes());
        }
        return byteBuffer.array();
    }
    /**
     * Takes a byte[] and decodes it into a SchemaRecord. This method assumes that the
     * input byte[] represents a d_record that corresponds to this schema.
     *
     * @param input the byte array to decode
     * @return the decoded SchemaRecord
     */
    public SchemaRecord decode(byte[] input) {
        int offset = 0;
        List<DataBox> values = new ArrayList<>();
        for (DataBox field : fieldTypes) {
            byte[] fieldBytes = Arrays.copyOfRange(input, offset, offset + field.getSize());
            offset += field.getSize();
            switch (field.type()) {
                case STRING:
                    values.add(new StringDataBox(fieldBytes));
                    break;
                case INT:
                    values.add(new IntDataBox(fieldBytes));
                    break;
                case FLOAT:
                    values.add(new FloatDataBox(fieldBytes));
                    break;
                case BOOL:
                    values.add(new BoolDataBox(fieldBytes));
                    break;
            }
        }
        return new SchemaRecord(values);
    }
    public int getEntrySize() {
        return this.size;
    }
    public List<String> getFieldNames() {
        return this.fields;
    }
    public List<DataBox> getFieldTypes() {
        return this.fieldTypes;
    }
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RecordSchema)) {
            return false;
        }
        RecordSchema otherSchema = (RecordSchema) other;
        if (this.fields.size() != otherSchema.fields.size()) {
            return false;
        }
        for (int i = 0; i < this.fields.size(); i++) {
            DataBox thisType = this.fieldTypes.get(i);
            DataBox otherType = otherSchema.fieldTypes.get(i);
            if (thisType.type() != otherType.type()) {
                return false;
            }
            if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
                return false;
            }
        }
        return true;
    }
}
