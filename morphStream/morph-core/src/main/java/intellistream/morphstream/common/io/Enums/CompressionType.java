package intellistream.morphstream.common.io.Enums;

public enum CompressionType {
    /**
     * Do not comprocess
     */
    UNCOMPRESSED("", (byte) 0),
    /**
     * SNAPPY
     */
    SNAPPY(".snappy", (byte) 1),
    /**
     * GZIP
     */
    GZIP(".gzip", (byte) 2),
    /**
     * LZ4
     */
    // NOTICE: To ensure the compatibility of existing files, do not change the byte LZ4 binds to.
    LZ4(".lz4", (byte) 7);

    private final String extensionName;
    private final byte index;

    CompressionType(String extensionName, byte index) {
        this.extensionName = extensionName;
        this.index = index;
    }

    /**
     * deserialize byte number.
     *
     * @param compressor byte number
     * @return CompressionType
     */
    public static CompressionType deserialize(byte compressor) {
        switch (compressor) {
            case 0:
                return CompressionType.UNCOMPRESSED;
            case 1:
                return CompressionType.SNAPPY;
            case 2:
                return CompressionType.GZIP;
            case 7:
                return CompressionType.LZ4;
            default:
                throw new IllegalArgumentException("Invalid input: " + compressor);
        }
    }

    public static int getSerializedSize() {
        return Byte.BYTES;
    }

    /**
     * @return byte number
     */
    public byte serialize() {
        return this.index;
    }

    /**
     * get extension.
     *
     * @return extension (string type), for example: .snappy, .gz, .lzo
     */
    public String getExtension() {
        return extensionName;
    }
}
