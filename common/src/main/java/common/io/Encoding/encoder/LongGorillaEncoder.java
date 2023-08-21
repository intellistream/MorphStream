package common.io.Encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static common.io.Utils.FileConfig.*;

public class LongGorillaEncoder extends GorillaEncoderV2{
    private static final int ONE_ITEM_MAX_SIZE =
            (2 +
                    LEADING_ZERO_BITS_LENGTH_64BIT +
                    MEANINGFUL_XOR_BITS_LENGTH_64BIT +
                    VALUE_BITS_LENGTH_64BIT)
            / Byte.SIZE +
                    1;
    private long storedValue = 0;

    @Override
    public final int getOneItemMaxSize() {
        return ONE_ITEM_MAX_SIZE;
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
        if (firstValueWasWritten) {
            compressValue(value, out);
        } else {
            writeFirst(value, out);
            firstValueWasWritten = true;
        }
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // ending stream
        encode(GORILLA_ENCODING_ENDING_LONG, out);

        // flip the byte no matter it is empty or not
        // the empty ending byte is necessary when decoding
        bitsLeft = 0;
        flipByte(out);

        // the encoder may be reused, so let us reset it
        reset();
    }

    @Override
    protected void reset() {
        super.reset();
        storedValue = 0;
    }

    private void writeFirst(long value, ByteArrayOutputStream out) {
        storedValue = value;
        writeBits(value, VALUE_BITS_LENGTH_64BIT, out);
    }

    private void compressValue(long value, ByteArrayOutputStream out) {
        long xor = storedValue ^ value;
        storedValue = value;

        if (xor == 0) {
            skipBit(out);
        } else {
            writeBit(out);
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);
            if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                writeExistingLeading(xor, out);
            } else {
                writeNewLeading(xor, leadingZeros, trailingZeros, out);
            }
        }
    }

    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control
     * bit = 0 (type a)
     *
     * <p>store the meaningful XORed value
     *
     * @param xor XOR between previous value and current
     */
    private void writeExistingLeading(long xor, ByteArrayOutputStream out) {
        skipBit(out);

        int significantBits = VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros;
        writeBits(xor >>> storedTrailingZeros, significantBits, out);
    }

    /**
     * Stores the length of the number of leading zeros in the next 6 bits
     *
     * <p>Stores the length of the meaningful XORed value in the next 6 bits
     *
     * <p>Stores the meaningful bits of the XORed value
     *
     * <p>(type b)
     *
     * @param xor XOR between previous value and current
     * @param leadingZeros New leading zeros
     * @param trailingZeros New trailing zeros
     */
    private void writeNewLeading(
            long xor, int leadingZeros, int trailingZeros, ByteArrayOutputStream out) {
        writeBit(out);

        int significantBits = VALUE_BITS_LENGTH_64BIT - leadingZeros - trailingZeros;

        // Number of leading zeros in the next 6 bits
        // Different from original, maximum number of leadingZeros is stored with 6 (original 5) bits to
        // allow up to 63 leading zeros, which are necessary when storing long values
        // Note that in this method the number of leading zeros won't be 64
        writeBits(leadingZeros, LEADING_ZERO_BITS_LENGTH_64BIT, out);
        // Length of meaningful bits in the next 6 bits
        // Note that in this method the number of meaningful bits is always positive and could be 64,
        // so we have to use (significantBits - 1) in storage
        writeBits((long) significantBits - 1, MEANINGFUL_XOR_BITS_LENGTH_64BIT, out);
        // Store the meaningful bits of XOR
        writeBits(xor >>> trailingZeros, significantBits, out);

        storedLeadingZeros = leadingZeros;
        storedTrailingZeros = trailingZeros;
    }

}
