package intellistream.morphstream.common.io.Encoding.encoder;

import intellistream.morphstream.common.io.Utils.FileConfig;

import java.io.ByteArrayOutputStream;

public class DoublePrecisionEncoderV1 extends GorillaEncoderV1 {
    private long preValue;

    public DoublePrecisionEncoderV1() {
        // do nothing
    }

    @Override
    public void encode(double value, ByteArrayOutputStream out) {
        if (!flag) {
            // case: write first 8 byte value without any encoding
            flag = true;
            preValue = Double.doubleToLongBits(value);
            leadingZeroNum = Long.numberOfLeadingZeros(preValue);
            tailingZeroNum = Long.numberOfTrailingZeros(preValue);
            byte[] bufferLittle = new byte[8];

            for (int i = 0; i < 8; i++) {
                bufferLittle[i] = (byte) (((preValue) >> (i * 8)) & 0xFF);
            }
            out.write(bufferLittle, 0, bufferLittle.length);
        } else {
            long nextValue = Double.doubleToLongBits(value);
            long tmp = nextValue ^ preValue;
            if (tmp == 0) {
                // case: write '0'
                writeBit(false, out);
            } else {
                int leadingZeroNumTmp = Long.numberOfLeadingZeros(tmp);
                int tailingZeroNumTmp = Long.numberOfTrailingZeros(tmp);
                if (leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum) {
                    // case: write '10' and effective bits without first leadingZeroNum '0'
                    // and last tailingZeroNum '0'
                    writeBit(true, out);
                    writeBit(false, out);
                    writeBits(
                            tmp, out, FileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNum, tailingZeroNum);
                } else {
                    // case: write '11', leading zero num of value, effective bits len and effective
                    // bit value
                    writeBit(true, out);
                    writeBit(true, out);
                    writeBits(leadingZeroNumTmp, out, FileConfig.LEADING_ZERO_BITS_LENGTH_64BIT - 1, 0);
                    writeBits(
                            (long) FileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNumTmp - tailingZeroNumTmp,
                            out,
                            FileConfig.DOUBLE_VALUE_LENGTH - 1,
                            0);
                    writeBits(
                            tmp,
                            out,
                            FileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNumTmp,
                            tailingZeroNumTmp);
                }
            }
            preValue = nextValue;
            leadingZeroNum = Long.numberOfLeadingZeros(preValue);
            tailingZeroNum = Long.numberOfTrailingZeros(preValue);
        }
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        encode(Double.NaN, out);
        clearBuffer(out);
        reset();
    }

    @Override
    public int getOneItemMaxSize() {
        // case '11'
        // 2bit + 6bit + 7bit + 64bit = 79bit
        return 10;
    }

    @Override
    public long getMaxByteSize() {
        // max(first 8 byte, case '11' 2bit + 6bit + 7bit + 64bit = 79bit )
        // + NaN(2bit + 6bit + 7bit + 64bit = 79bit) =
        // 158bit
        return 20;
    }

    private void writeBits(long num, ByteArrayOutputStream out, int start, int end) {
        for (int i = start; i >= end; i--) {
            long bit = num & (1L << i);
            writeBit(bit, out);
        }
    }
}
