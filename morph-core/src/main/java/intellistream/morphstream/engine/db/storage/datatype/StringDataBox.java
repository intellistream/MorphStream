package intellistream.morphstream.engine.db.storage.datatype;

import org.apache.commons.lang.StringUtils;

import java.nio.charset.StandardCharsets;

/**
 * Fixed-length String data type which serializes to UTF-8 bytes.
 */
public class StringDataBox extends DataBox {
    private String s;

    public StringDataBox(String s) {
        this.s = s;
    }

    /**
     * Construct an empty StringDataBox.
     */
    public StringDataBox() {
        this.s = "";
    }

    /**
     * Construct a StringDataBox with length len and value_list s.
     *
     * @param s   the value_list of the StringDataBox
     * @param len the length of the StringDataBox
     */
    public StringDataBox(String s, int len) {
        if (len < s.length()) {
            this.s = s.substring(0, len);
        } else {
            this.s = String.format("%-" + len + "s", s);
        }
    }

    /**
     * Construct a StringDataBox from the bytes in buf.
     *
     * @param buf the byte buffer source
     */
    public StringDataBox(byte[] buf) {
        this.s = new String(buf, StandardCharsets.UTF_8);
    }

    public StringDataBox(int len) {
        this.s = "";
        for (int i = 0; i < len; i++) {
            this.s += " ";
        }
    }

    @Override
    public StringDataBox clone() {
        return new StringDataBox(s);
    }

    @Override
    public String getString() {
        return this.s;
    }

    @Override
    public void setString(String s, int len) {
        this.s = StringUtils.rightPad(s, len);//String.format("%-" + len + "s", s);
    }

    @Override
    public Types type() {
        return DataBox.Types.STRING;
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
        StringDataBox other = (StringDataBox) obj;
        return this.getString().equals(other.getString());
    }

    @Override
    public int hashCode() {
        return this.getString().hashCode();
    }

    @Override
    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            throw new DataBoxException("Invalid Comparsion");
        }
        StringDataBox other = (StringDataBox) obj;
        return this.getString().compareTo(other.getString());
    }

    @Override
    public byte[] getBytes() {
        return this.s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int getSize() {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public String toString() {
        return this.s;
    }
}
