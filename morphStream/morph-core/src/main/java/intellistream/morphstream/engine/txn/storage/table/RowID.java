package intellistream.morphstream.engine.txn.storage.table;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents the ID of a partition d_record. Stores the id of a page and the slot number select this
 * d_record lives within that page.
 */
public class RowID {
    private final int id;

    public RowID(int id) {
        this.id = id;
    }

    public RowID(byte[] buff) {
        ByteBuffer bb = ByteBuffer.wrap(buff);
        this.id = bb.getInt();
    }

    static public int getSize() {
        return 8;
    }

    @Override
    public String toString() {
        return "(RowID: " + this.id + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RowID)) {
            return false;
        }
        RowID otherRecord = (RowID) other;
        return otherRecord.getID() == this.getID();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getID());
    }

    public byte[] getBytes() {
        return ByteBuffer.allocate(8).putLong(getID()).array();
    }

    public int compareTo(Object obj) {
        RowID other = (RowID) obj;
        return Long.compare(this.getID(), other.getID());
    }

    public int getID() {
        return id;
    }
}
