package intellistream.morphstream.common.io.Enums;

import intellistream.morphstream.common.io.Exception.write.UnSupportedDataTypeException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum DataType {
    /**
     * BOOLEAN
     */
    BOOLEAN((byte) 0),

    /**
     * INT32
     */
    INT32((byte) 1),

    /**
     * INT64
     */
    INT64((byte) 2),

    /**
     * FLOAT
     */
    FLOAT((byte) 3),

    /**
     * DOUBLE
     */
    DOUBLE((byte) 4),

    /**
     * TEXT
     */
    TEXT((byte) 5),

    /**
     * VECTOR
     */
    VECTOR((byte) 6);

    private final byte type;

    DataType(byte type) {
        this.type = type;
    }

    /**
     * give an integer to return a data type.
     *
     * @param type -param to judge enum type
     * @return -enum type
     */
    public static DataType deserialize(byte type) {
        return getTsDataType(type);
    }

    public static DataType getTsDataType(byte type) {
        switch (type) {
            case 0:
                return DataType.BOOLEAN;
            case 1:
                return DataType.INT32;
            case 2:
                return DataType.INT64;
            case 3:
                return DataType.FLOAT;
            case 4:
                return DataType.DOUBLE;
            case 5:
                return DataType.TEXT;
            case 6:
                return DataType.VECTOR;
            default:
                throw new IllegalArgumentException("Invalid input: " + type);
        }
    }

    public static DataType deserializeFrom(ByteBuffer buffer) {
        return deserialize(buffer.get());
    }

    public static int getSerializedSize() {
        return Byte.BYTES;
    }

    public byte getType() {
        return type;
    }

    public void serializeTo(ByteBuffer byteBuffer) {
        byteBuffer.put(serialize());
    }

    public void serializeTo(DataOutputStream outputStream) throws IOException {
        outputStream.write(serialize());
    }

    public int getDataTypeSize() {
        switch (this) {
            case BOOLEAN:
                return 1;
            case INT32:
            case FLOAT:
                return 4;
            // For text: return the size of reference here
            case TEXT:
            case INT64:
            case DOUBLE:
            case VECTOR:
                return 8;
            default:
                throw new UnSupportedDataTypeException(this.toString());
        }
    }

    /**
     * @return byte number
     */
    public byte serialize() {
        return type;
    }

    /**
     * @return whether a numeric datatype
     */
    public boolean isNumeric() {
        switch (this) {
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
                return true;
            // For text: return the size of reference here
            case BOOLEAN:
            case TEXT:
            case VECTOR:
                return false;
            default:
                throw new UnSupportedDataTypeException(this.toString());
        }
    }
}
