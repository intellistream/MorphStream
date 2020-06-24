package state_engine.storage.datatype;

import state_engine.storage.SchemaRecord;

import java.nio.ByteBuffer;

/**
 * Long data type which serializes to 8 bytes
 */
public class LongDataBox extends DataBox {
    private long i;

    /**
     * Construct an empty LongDataBox with value_list 0.
     */
    public LongDataBox() {
        this.i = 0;
    }

    /**
     * Constructs an LongDataBox with value_list i.
     *
     * @param i the value_list of the LongDataBox
     */
    public LongDataBox(long i) {
        this.i = i;
    }

    /**
     * Construct an LongDataBox from the bytes in buf.
     *
     * @param buf the byte buffer source
     */
    public LongDataBox(byte[] buf) {
        if (buf.length != this.getSize()) {
            throw new DataBoxException("Wrong size buffer for long");
        }
        this.i = ByteBuffer.wrap(buf).getLong();
    }

    @Override
    public LongDataBox clone() {
        return new LongDataBox(i);
    }

    @Override
    public long getLong() {
        return this.i;
    }


    @Override
    public void setLong(long i) {
        this.i = i;
    }


    /**
     * MVCC to make sure it's reading the correct version..
     *
     * @param s_record
     * @param delta
     */
    @Override
    public void incLong(SchemaRecord s_record, long delta) {
        this.i = s_record.getValues().get(1).getLong() + delta;
    }

    @Override
    public void incLong(long current_value, long delta) {
        this.i = current_value + delta;
    }

    @Override
    public void incLong(long delta) {
        this.i = i + delta;
    }


    @Override
    public void decLong(SchemaRecord s_record, long delta) {
        this.i = s_record.getValues().get(1).getLong() + delta;
    }

    public void decLong(long current_value, long delta) {
        this.i = current_value - delta;
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
        LongDataBox other = (LongDataBox) obj;
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
        LongDataBox other = (LongDataBox) obj;
        return Long.compare(this.getLong(), other.getLong());
    }

    @Override
    public byte[] getBytes() {
        return ByteBuffer.allocate(8).putLong(this.i).array();
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
