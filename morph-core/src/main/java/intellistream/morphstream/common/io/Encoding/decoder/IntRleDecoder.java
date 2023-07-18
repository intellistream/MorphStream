package intellistream.morphstream.common.io.Encoding.decoder;

import intellistream.morphstream.common.io.Encoding.bitpacker.IntPacker;
import intellistream.morphstream.common.io.Exception.encoding.DecodingException;
import intellistream.morphstream.common.io.Utils.FileConfig;
import intellistream.morphstream.common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IntRleDecoder extends RleDecoder {
    private static final Logger logger = LoggerFactory.getLogger(IntRleDecoder.class);

    /**
     * current value for rle repeated value.
     */
    private int currentValue;

    /**
     * buffer to save all values in group using bit-packing.
     */
    private int[] currentBuffer;

    /**
     * packer for unpacking int values.
     */
    private IntPacker packer;

    public IntRleDecoder() {
        super();
        currentValue = 0;
    }

    @Override
    public boolean readBoolean(ByteBuffer buffer) {
        return this.readInt(buffer) != 0;
    }

    /**
     * read an int value from InputStream.
     *
     * @param buffer - ByteBuffer
     * @return value - current valid value
     */
    @Override
    public int readInt(ByteBuffer buffer) {
        if (!isLengthAndBitWidthReaded) {
            // start to read a new rle+bit-packing pattern
            readLengthAndBitWidth(buffer);
        }

        if (currentCount == 0) {
            try {
                readNext();
            } catch (IOException e) {
                logger.error(
                        "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                                + " length is {}, bit width is {}",
                        length,
                        bitWidth,
                        e);
            }
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case BIT_PACKED:
                result = currentBuffer[bitPackingNum - currentCount - 1];
                break;
            default:
                throw new DecodingException(
                        String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
        }

        if (!hasNextPackage()) {
            isLengthAndBitWidthReaded = false;
        }
        return result;
    }

    @Override
    protected void initPacker() {
        packer = new IntPacker(bitWidth);
    }

    @Override
    protected void readNumberInRle() throws IOException {
        currentValue =
                ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(byteCache, bitWidth);
    }

    @Override
    protected void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum) {
        currentBuffer = new int[bitPackedGroupCount * FileConfig.RLE_MIN_REPEATED_NUM];
        byte[] bytes = new byte[bitPackedGroupCount * bitWidth];
        int bytesToRead = bitPackedGroupCount * bitWidth;
        bytesToRead = Math.min(bytesToRead, byteCache.remaining());
        byteCache.get(bytes, 0, bytesToRead);

        // save all int values in currentBuffer
        packer.unpackAllValues(bytes, bytesToRead, currentBuffer);
    }
}
