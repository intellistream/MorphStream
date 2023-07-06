package common.io.Encoding.decoder;

import java.nio.ByteBuffer;

import static common.io.Utils.FileConfig.*;

public class IntGorillaDecoder extends GorillaDecoderV2{
    protected int storedValue = 0;
    @Override
    public void reset() {
        super.reset();
        storedValue = 0;
    }

    @Override
    public final int readInt(ByteBuffer in) {
        int returnValue = storedValue;
        if (!firstValueWasRead) {
            flipByte(in);
            storedValue = (int) readLong(VALUE_BITS_LENGTH_32BIT, in);
            firstValueWasRead = true;
            returnValue = storedValue;
        }
        cacheNext(in);
        return returnValue;
    }
    protected int cacheNext(ByteBuffer in) {
        readNext(in);
        if (storedValue == GORILLA_ENCODING_ENDING_INTEGER) {
            hasNext = false;
        }
        return storedValue;
    }

    @SuppressWarnings("squid:S128")
    protected int readNext(ByteBuffer in) {
        byte controlBits = readNextClearBit(2, in);

        switch (controlBits) {
            case 3: // case '11': use new leading and trailing zeros
                storedLeadingZeros = (int) readLong(LEADING_ZERO_BITS_LENGTH_32BIT, in);
                byte significantBits = (byte) readLong(MEANINGFUL_XOR_BITS_LENGTH_32BIT, in);
                significantBits++;
                storedTrailingZeros = VALUE_BITS_LENGTH_32BIT - significantBits - storedLeadingZeros;
                // missing break is intentional, we want to overflow to next one
            case 2: // case '10': use stored leading and trailing zeros
                int xor =
                        (int) readLong(VALUE_BITS_LENGTH_32BIT - storedLeadingZeros - storedTrailingZeros, in);
                xor <<= storedTrailingZeros;
                storedValue ^= xor;
                // missing break is intentional, we want to overflow to next one
            default: // case '0': use stored value
                return storedValue;
        }
    }
}
