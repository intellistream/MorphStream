package intellistream.morphstream.common.io.Encoding.encoder;

import intellistream.morphstream.common.io.Enums.Encoding;

import java.io.ByteArrayOutputStream;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project implemented
 * in Iotdb
 * <p>Project page: https://iotdb.apache.org/
 * <p>Code page: https://github.com/apache/iotdb
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public abstract class GorillaEncoderV2 extends Encoder {
    protected boolean firstValueWasWritten = false;
    protected int storedLeadingZeros = Integer.MAX_VALUE;
    protected int storedTrailingZeros = 0;
    protected int bitsLeft = Byte.SIZE;
    private byte buffer = 0;

    protected GorillaEncoderV2() {
        super(Encoding.GORILLA);
    }

    @Override
    public final long getMaxByteSize() {
        return 0;
    }

    protected void reset() {
        firstValueWasWritten = false;
        storedLeadingZeros = Integer.MAX_VALUE;
        storedTrailingZeros = 0;

        buffer = 0;
        bitsLeft = Byte.SIZE;
    }

    /**
     * Stores a 0 and increases the count of bits by 1
     */
    protected void skipBit(ByteArrayOutputStream out) {
        bitsLeft--;
        flipByte(out);
    }

    /**
     * Stores a 1 and increases the count of bits by 1
     */
    protected void writeBit(ByteArrayOutputStream out) {
        buffer |= (1 << (bitsLeft - 1));
        bitsLeft--;
        flipByte(out);
    }

    /**
     * Writes the given long value using the defined amount of least significant bits.
     *
     * @param value The long value to be written
     * @param bits  How many bits are stored to the stream
     */
    protected void writeBits(long value, int bits, ByteArrayOutputStream out) {
        while (bits > 0) {
            int shift = bits - bitsLeft;
            if (shift >= 0) {
                buffer |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                shift = bitsLeft - bits;
                buffer |= (byte) (value << shift);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte(out);
        }
    }

    protected void flipByte(ByteArrayOutputStream out) {
        if (bitsLeft == 0) {
            out.write(buffer);
            buffer = 0;
            bitsLeft = Byte.SIZE;
        }
    }
}
