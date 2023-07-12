package common.io.Encoding.encoder;

import common.io.Encoding.bitpacker.IntPacker;
import common.io.Utils.FileConfig;
import common.io.Utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/** Encoder for int value using rle or bit-packing. */
public class IntRleEncoder extends RleEncoder<Integer>{
    /** Packer for packing int values. */
    private IntPacker packer;

    public IntRleEncoder() {
        super();
        bufferedValues = new Integer[FileConfig.RLE_MIN_REPEATED_NUM];
        preValue = 0;
        values = new ArrayList<>();
    }
    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        values.add(value);
    }

    @Override
    public void encode(boolean value, ByteArrayOutputStream out) {
        if (value) {
            this.encode(1, out);
        } else {
            this.encode(0, out);
        }
    }

    /**
     * write all values buffered in the cache to an OutputStream.
     *
     * @param out - byteArrayOutputStream
     * @throws IOException cannot flush to OutputStream
     */
    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // we get bit width after receiving all data
        this.bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(values);
        packer = new IntPacker(bitWidth);
        for (Integer value : values) {
            encodeValue(value);
        }
        super.flush(out);
    }

    @Override
    protected void reset() {
        super.reset();
        preValue = 0;
    }
    /** write bytes to an outputStream using rle format: [header][value]. */
    @Override
    protected void writeRleRun() throws IOException {
        endPreviousBitPackedRun(FileConfig.RLE_MIN_REPEATED_NUM);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(repeatCount << 1, byteCache);
        ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(preValue, byteCache, bitWidth);
        repeatCount = 0;
        numBufferedValues = 0;
    }

    @Override
    protected void clearBuffer() {

        for (int i = numBufferedValues; i < FileConfig.RLE_MIN_REPEATED_NUM; i++) {
            bufferedValues[i] = 0;
        }
    }

    @Override
    protected void convertBuffer() {
        byte[] bytes = new byte[bitWidth];

        int[] tmpBuffer = new int[FileConfig.RLE_MIN_REPEATED_NUM];
        for (int i = 0; i < FileConfig.RLE_MIN_REPEATED_NUM; i++) {
            tmpBuffer[i] = (int) bufferedValues[i];
        }
        packer.pack8Values(tmpBuffer, 0, bytes);
        // we'll not write bit-packing group to OutputStream immediately
        // we buffer them in list
        bytesBuffer.add(bytes);
    }

    @Override
    public int getOneItemMaxSize() {
        // The meaning of 45 is:
        // 4 + 4 + max(4+4,1 + 4 + 4 * 8)
        // length + bitwidth + max(rle-header + num, bit-header + lastNum + 8packer)
        return 45;
    }

    @Override
    public long getMaxByteSize() {
        if (values == null) {
            return 0;
        }
        // try to caculate max value
        int groupNum = (values.size() / 8 + 1) / 63 + 1;
        return (long) 8 + groupNum * 5 + values.size() * 4;
    }
}
