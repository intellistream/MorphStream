package common.io.Enums;

public enum Encoding {
    PLAIN((byte) 0),
    DICTIONARY((byte) 1),
    RLE((byte) 2),
    DIFF((byte) 3),
    TS_2DIFF((byte) 4),
    BITMAP((byte) 5),
    GORILLA_V1((byte) 6),
    REGULAR((byte) 7),
    GORILLA((byte) 8),
    ZIGZAG((byte) 9),
    FREQ((byte) 10);

    private final byte type;

    Encoding(byte type) {
        this.type = type;
    }

    /**
     * judge the encoding deserialize type.
     *
     * @param encoding -use to determine encoding type
     * @return -encoding type
     */
    public static Encoding deserialize(byte encoding) {
        return getEncoding(encoding);
    }

    private static Encoding getEncoding(byte encoding) {
        switch (encoding) {
            case 0:
                return Encoding.PLAIN;
            case 1:
                return Encoding.DICTIONARY;
            case 2:
                return Encoding.RLE;
            case 3:
                return Encoding.DIFF;
            case 4:
                return Encoding.TS_2DIFF;
            case 5:
                return Encoding.BITMAP;
            case 6:
                return Encoding.GORILLA_V1;
            case 7:
                return Encoding.REGULAR;
            case 8:
                return Encoding.GORILLA;
            case 9:
                return Encoding.ZIGZAG;
            case 10:
                return Encoding.FREQ;
            default:
                throw new IllegalArgumentException("Invalid input: " + encoding);
        }
    }

    public static int getSerializedSize() {
        return Byte.BYTES;
    }

    /**
     * judge the encoding deserialize type.
     *
     * @return -encoding type
     */
    public byte serialize() {
        return type;
    }
}
