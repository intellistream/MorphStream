package state_engine.storage.datatype;

import org.apache.commons.lang3.SerializationUtils;


/**
 * Long data type which serializes to 8 bytes
 */
public class TimeStampDataBox extends DataBox {
    private TimestampType i;

    /**
     * Construct an empty LongDataBox with value_list 0.
     */
    public TimeStampDataBox() {
        this.i = new TimestampType();
    }

    /**
     * Constructs an LongDataBox with value_list i.
     *
     * @param i the value_list of the LongDataBox
     */
    public TimeStampDataBox(TimestampType i) {
        this.i = i;
    }

    /**
     * Construct an LongDataBox from the bytes in buf.
     *
     * @param buf the byte buffer source
     */
    public TimeStampDataBox(byte[] buf) {
        if (buf.length != this.getSize()) {
            throw new DataBoxException("Wrong size buffer for long");
        }
        this.i = SerializationUtils.deserialize(buf); //ByteBuffer.wrap(buf).get;
    }

    @Override
    public TimeStampDataBox clone() {
        return new TimeStampDataBox(i);
    }

    public TimestampType getTimestamp() {
        return this.i;
    }

    @Override
    public void setTimestamp(TimestampType i) {
        this.i = i;
    }

    @Override
    public Types type() {
        return Types.LONG;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (this == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        TimeStampDataBox other = (TimeStampDataBox) obj;
        return this.getLong() == other.getLong();
    }

    @Override
    public int hashCode() {
        return (int) Math.abs(this.getLong());
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        TimeStampDataBox other = (TimeStampDataBox) obj;
        return Long.compare(this.getLong(), other.getLong());
    }

    @Override
    public byte[] getBytes() {
        return
                SerializationUtils.serialize(this.i);
        //ByteBuffer.allocate(8).putLong(this.i).array();
    }

    @Override
    public int getSize() {
        return 8;
    }

    @Override
    public String toString() {
        return "" + this.i;
    }
}
