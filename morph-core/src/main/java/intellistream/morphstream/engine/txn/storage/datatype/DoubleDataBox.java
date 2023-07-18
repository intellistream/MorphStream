package intellistream.morphstream.engine.txn.storage.datatype;

import java.nio.ByteBuffer;

/**
 * Float data type which serializes to 14 bytes.
 */
public class DoubleDataBox extends DataBox {
    private volatile double d;

    /**
     * Construct an empty FloatDataBox with value_list 0.
     */
    public DoubleDataBox() {
        this.d = 0.0d;
    }

    /**
     * Construct an empty FloatDataBox with value_list d.
     *
     * @param f the value_list of the FloatDataBox
     */
    public DoubleDataBox(double f) {
        this.d = f;
    }

    /**
     * Construct a FloatDataBox from the bytes in buf
     *
     * @param buf the bytes to construct the FloatDataBox from
     */
    public DoubleDataBox(byte[] buf) {
        if (buf.length != this.getSize()) {
            throw new DataBoxException("Wrong size buffer for float");
        }
        this.d = ByteBuffer.wrap(buf).getFloat();
    }

    @Override
    public DoubleDataBox clone() {
        return new DoubleDataBox(d);
    }

    @Override
    public double getDouble() {
        return this.d;
    }

    @Override
    public void setDouble(double d) {
        this.d = d;
    }

    @Override
    public Types type() {
        return Types.FLOAT;
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
        DoubleDataBox other = (DoubleDataBox) obj;
        return this.getFloat() == other.getFloat();
    }

    @Override
    public int hashCode() {
        return (int) this.getFloat();
    }

    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        DoubleDataBox other = (DoubleDataBox) obj;
        return Double.compare(this.getDouble(), other.getDouble());
    }

    @Override
    public byte[] getBytes() {
        return ByteBuffer.allocate(8).putDouble(this.d).array();
    }

    @Override
    public int getSize() {
        return 8;
    }

    @Override
    public String toString() {
        return String.valueOf(this.d);
    }
}

