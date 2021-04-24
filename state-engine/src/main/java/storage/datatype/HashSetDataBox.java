package storage.datatype;
import java.util.HashSet;
/**
 * Integer data type which serializes to 4 bytes
 */
public class HashSetDataBox extends DataBox {
    private volatile HashSet set;
    /**
     * Construct an empty IntDataBox with value_list 0.
     */
    public HashSetDataBox() {
        this.set = new HashSet();
    }
    public HashSetDataBox(HashSet set) {
        this.set = set;
    }
    @Override
    public HashSetDataBox clone() {
        return new HashSetDataBox(set);
    }
    @Override
    public Types type() {
        return Types.OTHERS;
    }
    public HashSet getHashSet() {
        return set;
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
        HashSetDataBox other = (HashSetDataBox) obj;
        return this.getInt() == other.getInt();
    }
    @Override
    public int hashCode() {
        return Math.abs(this.getInt());
    }
    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        HashSetDataBox other = (HashSetDataBox) obj;
        return Integer.compare(this.getInt(), other.getInt());
    }
    @Override
    public byte[] getBytes() {
        //not applicable.
        return null;
    }
    @Override
    public int getSize() {
        return -1;
    }
    @Override
    public String toString() {
        return "" + this.set;
    }
}
