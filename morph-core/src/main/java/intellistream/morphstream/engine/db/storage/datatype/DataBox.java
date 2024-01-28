package intellistream.morphstream.engine.db.storage.datatype;

import intellistream.morphstream.engine.db.storage.SchemaRecord;

import java.util.HashSet;
import java.util.List;

/**
 * Abstract DataBox for all database primitives Currently supported: integers, booleans, floats,
 * fixed-length strings.
 * <p>
 * DataBoxes are also comparable allowing comparisons or sorting.
 * <p>
 * Provides default functionality for all DataBox subclasses by assuming that the contained value_list is
 * not of the type specified.
 */
public abstract class DataBox implements Comparable, Cloneable {
    public DataBox() throws DataBoxException {

    }

    public DataBox(boolean b) throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }

    public DataBox(int i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public DataBox(float f) throws DataBoxException {
        throw new DataBoxException("not float type");
    }

    public DataBox(String s, int len) throws DataBoxException {
        throw new DataBoxException("not String type");
    }

    public DataBox(byte[] buf) throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }

    @Override
    public DataBox clone() {
        throw new DataBoxException("not cloneable");
    }

    public boolean getBool() throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }

    public void setBool(boolean b) throws DataBoxException {
        throw new DataBoxException("not boolean type");
    }

    public int getInt() throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public void setInt(int i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public long getLong() throws DataBoxException {
        throw new DataBoxException("not long type");
    }

    public void setLong(long i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public TimestampType getTimestamp() throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public void setTimestamp(TimestampType i) throws DataBoxException {
        throw new DataBoxException("not int type");
    }

    public float getFloat() throws DataBoxException {
        throw new DataBoxException("not float type");
    }

    public void setFloat(float f) throws DataBoxException {
        throw new DataBoxException("not float type");
    }

    public List<Double> getDoubleList() {
        throw new DataBoxException("not list type");
    }

    public void setString(String s, int len) throws DataBoxException {
        throw new DataBoxException("not string type");
    }

    public void incLong(long current_value, long delta) {
        throw new DataBoxException("not string type");
    }

    public void incLong(SchemaRecord s_record, long delta) {
        throw new DataBoxException("not string type");
    }

    public void incLong(long delta) {
        incLong(getLong(), delta);
    }

    public void decLong(SchemaRecord s_record, long delta) {
        throw new DataBoxException("not string type");
    }

    public void decLong(long current_value, long delta) {
        throw new DataBoxException("not string type");
    }

    public double getDouble() {
        throw new DataBoxException("not double type");
    }

    public void setDouble(double f) {
        throw new DataBoxException("not double type");
    }

    public double addItem(Double nextDouble) {
        throw new DataBoxException("not list type");
    }

    /**
     * Returns the type of the DataBox.
     *
     * @return the type from the Types enum
     * @throws DataBoxException
     */
    public Types type() throws DataBoxException {
        throw new DataBoxException("No type");
    }

    /**
     * Returns a byte array with the data contained by this DataBox.
     *
     * @return a byte array
     * @throws DataBoxException
     */
    public byte[] getBytes() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }

    /**
     * Returns the fixed size of this DataBox.
     *
     * @return the size of the DataBox
     * @throws DataBoxException
     */
    public int getSize() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }

    public int compareTo(Object obj) throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }

    @Override
    public String toString() throws DataBoxException {
        throw new DataBoxException("Not Implemented");
    }

    /**
     * All data must be able to provide a string representation for indexing purpose...
     * TODO: think about better way later.
     *
     * @return
     */
    public String getString() {
        if (this instanceof BoolDataBox) {
            return String.valueOf((this).getBool());
        } else if (this instanceof FloatDataBox) {
            return String.valueOf((this).getFloat());
        } else if (this instanceof IntDataBox) {
            return String.valueOf((this).getInt());
        } else if (this instanceof LongDataBox) {
            return String.valueOf((this).getLong());
        } else if (this instanceof TimeStampDataBox) {
            return String.valueOf((this).getTimestamp());
        }
        throw new DataBoxException("Not Implemented");
    }

    public HashSet getHashSet() {
        throw new DataBoxException("Not Implemented");
    }

    /**
     * An enum with the current supported types.
     */
    public enum Types {
        BOOL, INT, LONG, TimestampType, FLOAT, DOUBLE, STRING, OTHERS
    }
}
