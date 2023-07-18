package common.io.Utils;

import java.io.Serializable;
import java.util.Arrays;
/**
 * Override compareTo() and equals() function to Binary class. This class is used to accept Java
 * String type
 */
public class Binary implements Comparable<Binary>, Serializable {

    private static final long serialVersionUID = 6394197743397020735L;
    public static final Binary EMPTY_VALUE = new Binary("");

    private byte[] values;

    /** if the bytes v is modified, the modification is visible to this binary. */
    public Binary(byte[] v) {
        this.values = v;
    }

    public Binary(String s) {
        this.values = (s == null) ? null : s.getBytes(FileConfig.STRING_CHARSET);
    }

    public static Binary valueOf(String value) {
        return new Binary(BytesUtils.stringToBytes(value));
    }

    @Override
    public int compareTo(Binary other) {
        if (other == null) {
            if (this.values == null) {
                return 0;
            } else {
                return 1;
            }
        }

        int i = 0;
        while (i < getLength() && i < other.getLength()) {
            if (this.values[i] == other.values[i]) {
                i++;
                continue;
            }
            return this.values[i] - other.values[i];
        }
        return getLength() - other.getLength();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }

        return compareTo((Binary) other) == 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    /**
     * get length.
     *
     * @return length
     */
    public int getLength() {
        if (this.values == null) {
            return -1;
        }
        return this.values.length;
    }

    public String getStringValue() {
        return new String(this.values, FileConfig.STRING_CHARSET);
    }

    public String getTextEncodingType() {
        return FileConfig.STRING_ENCODING;
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    public byte[] getValues() {
        return values;
    }

    public void setValues(byte[] values) {
        this.values = values;
    }
}
