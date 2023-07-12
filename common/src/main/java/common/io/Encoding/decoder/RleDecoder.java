package common.io.Encoding.decoder;

import common.io.Enums.Encoding;
import common.io.Exception.encoding.DecodingException;
import common.io.Utils.Binary;
import common.io.Utils.FileConfig;
import common.io.Utils.ReadWriteForEncodingUtils;
import common.io.Utils.ReadWriteIOUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Abstract class for all rle decoder. Decoding values according to following grammar: {@code
 * <length> <bitwidth> <encoded-data>}. For more information about rle format, see RleEncoder
 */
public abstract class RleDecoder extends Decoder{
    /** mode to indicate current encoding type 0 - RLE 1 - BIT_PACKED. */
    protected Mode mode;
    /** bit width for bit-packing and rle to decode. */
    protected int bitWidth;
    /** number of data left for reading in current buffer. */
    protected int currentCount;
    /**
     * how many bytes for all encoded data like [{@code <bitwidth> <encoded-data>}] in inputstream.
     */
    protected int length;
    /**
     * a flag to indicate whether current pattern is end. false - need to start reading a new page
     * true - current page isn't over.
     */
    protected boolean isLengthAndBitWidthReaded;
    /** buffer to save data format like [{@code <bitwidth> <encoded-data>}] for decoder. */
    protected ByteBuffer byteCache;
    /** number of bit-packing group in which is saved in header. */
    protected int bitPackingNum;

    /** a constructor, init with endianType, default encoding is <code>TSEncoding.RLE</code>. */
    protected RleDecoder() {
        super(Encoding.RLE);
        reset();
    }

    @Override
    public void reset() {
        currentCount = 0;
        isLengthAndBitWidthReaded = false;
        bitPackingNum = 0;
        byteCache = ByteBuffer.allocate(0);
    }

    /**
     * get header for both rle and bit-packing current encode mode which is saved in first bit of
     * header.
     *
     * @return int value
     * @throws IOException cannot get header
     */
    public int getHeader() throws IOException {
        int header = ReadWriteForEncodingUtils.readUnsignedVarInt(byteCache);
        mode = (header & 1) == 0 ? Mode.RLE : Mode.BIT_PACKED;
        return header;
    }

    /**
     * get all encoded data according to mode.
     *
     * @throws IOException cannot read next value
     */
    protected void readNext() throws IOException {
        int header = getHeader();
        switch (mode) {
            case RLE:
                currentCount = header >> 1;
                readNumberInRle();
                break;
            case BIT_PACKED:
                callReadBitPackingBuffer(header);
                break;
            default:
                throw new DecodingException(
                        String.format("tsfile-encoding IntRleDecoder: unknown encoding mode %s", mode));
        }
    }

    protected void callReadBitPackingBuffer(int header) throws IOException {
        int bitPackedGroupCount = header >> 1;
        // in last bit-packing group, there may be some useless value,
        // lastBitPackedNum indicates how many values is useful
        int lastBitPackedNum = ReadWriteIOUtils.read(byteCache);
        if (bitPackedGroupCount > 0) {

            currentCount =
                    (bitPackedGroupCount - 1) * FileConfig.RLE_MIN_REPEATED_NUM + lastBitPackedNum;
            bitPackingNum = currentCount;
        } else {
            throw new DecodingException(
                    String.format(
                            "tsfile-encoding IntRleDecoder: bitPackedGroupCount %d, smaller than 1",
                            bitPackedGroupCount));
        }
        readBitPackingBuffer(bitPackedGroupCount, lastBitPackedNum);
    }

    /**
     * read length and bit width of current package before we decode number.
     *
     * @param buffer ByteBuffer
     */
    protected void readLengthAndBitWidth(ByteBuffer buffer) {
        length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        byte[] tmp = new byte[length];
        buffer.get(tmp, 0, length);
        byteCache = ByteBuffer.wrap(tmp);
        isLengthAndBitWidthReaded = true;
        bitWidth = ReadWriteIOUtils.read(byteCache);
        initPacker();
    }

    /**
     * Check whether there is number left for reading.
     *
     * @param buffer decoded data saved in ByteBuffer
     * @return true or false to indicate whether there is number left
     * @throws IOException cannot check next value
     */
    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        if (currentCount > 0 || buffer.remaining() > 0 || hasNextPackage()) {
            return true;
        }
        return false;
    }

    /**
     * Check whether there is another pattern left for reading.
     *
     * @return true or false to indicate whether there is another pattern left
     */
    protected boolean hasNextPackage() {
        return currentCount > 0 || byteCache.remaining() > 0;
    }

    protected abstract void initPacker();

    /**
     * Read rle package and save them in buffer.
     *
     * @throws IOException cannot read number
     */
    protected abstract void readNumberInRle() throws IOException;

    /**
     * Read bit-packing package and save them in buffer.
     *
     * @param bitPackedGroupCount number of group number
     * @param lastBitPackedNum number of useful value in last group
     * @throws IOException cannot read bit pack
     */
    protected abstract void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum)
            throws IOException;

    @Override
    public boolean readBoolean(ByteBuffer buffer) {
        throw new DecodingException("Method readBoolean is not supproted by RleDecoder");
    }

    @Override
    public short readShort(ByteBuffer buffer) {
        throw new DecodingException("Method readShort is not supproted by RleDecoder");
    }

    @Override
    public int readInt(ByteBuffer buffer) {
        throw new DecodingException("Method readInt is not supproted by RleDecoder");
    }

    @Override
    public long readLong(ByteBuffer buffer) {
        throw new DecodingException("Method readLong is not supproted by RleDecoder");
    }

    @Override
    public float readFloat(ByteBuffer buffer) {
        throw new DecodingException("Method readFloat is not supproted by RleDecoder");
    }

    @Override
    public double readDouble(ByteBuffer buffer) {
        throw new DecodingException("Method readDouble is not supproted by RleDecoder");
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        throw new DecodingException("Method readBinary is not supproted by RleDecoder");
    }

    @Override
    public BigDecimal readBigDecimal(ByteBuffer buffer) {
        throw new DecodingException("Method readBigDecimal is not supproted by RleDecoder");
    }

    protected enum Mode {
        RLE,
        BIT_PACKED
    }
}
