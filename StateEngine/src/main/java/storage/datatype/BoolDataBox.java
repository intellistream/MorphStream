package storage.datatype;
import java.nio.ByteBuffer;
/**
 * Boolean data type which serializes to 1 byte.
 */
public class BoolDataBox extends DataBox {
    private volatile boolean bool;
    /**
     * Construct an empty BoolDataBox.
     */
    public BoolDataBox() {
        this.bool = false;
    }
    /**
     * Construct a BoolDataBox with value_list b.
     *
     * @param b the value_list of the BoolDataBox
     */
    public BoolDataBox(boolean b) {
        this.bool = b;
    }
    /**
     * Construct a BoolDataBox from a byte buffer.
     *
     * @param buf the byte buffer source
     */
    public BoolDataBox(byte[] buf) {
        if (buf.length != this.getSize()) {
            throw new DataBoxException("Wrong size buffer for boolean");
        }
        this.bool = (buf[0] != 0);
    }
    @Override
    public BoolDataBox clone() {
        return new BoolDataBox(bool);
    }
    @Override
    public boolean getBool() {
        return this.bool;
    }
    @Override
    public void setBool(boolean b) {
        this.bool = b;
    }
    @Override
    public Types type() {
        return DataBox.Types.BOOL;
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
        BoolDataBox other = (BoolDataBox) obj;
        return Boolean.compare(this.getBool(), other.getBool()) == 0;
    }
    @Override
    public int hashCode() {
        return this.getBool() ? 1 : 0;
    }
    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        BoolDataBox other = (BoolDataBox) obj;
        return Boolean.compare(this.getBool(), other.getBool());
    }
    @Override
    public byte[] getBytes() {
        byte val = this.bool ? (byte) 1 : (byte) 0;
        return ByteBuffer.allocate(1).put(val).array();
    }
    @Override
    public int getSize() {
        return 1;
    }
    @Override
    public String toString() {
        if (this.bool) {
            return "true";
        } else {
            return "false";
        }
    }
}
