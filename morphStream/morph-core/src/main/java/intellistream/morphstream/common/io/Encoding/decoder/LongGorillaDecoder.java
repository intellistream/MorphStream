package intellistream.morphstream.common.io.Encoding.decoder;

import java.nio.ByteBuffer;

import static intellistream.morphstream.common.io.Utils.FileConfig.*;

public class LongGorillaDecoder extends GorillaDecoderV2 {
    protected long storedValue = 0;

    @Override
    public void reset() {
        super.reset();
        storedValue = 0;
    }

    @Override
    public final long readLong(ByteBuffer in) {
        long returnValue = storedValue;
        if (!firstValueWasRead) {
            flipByte(in);
            storedValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
            firstValueWasRead = true;
            returnValue = storedValue;
        }
        cacheNext(in);
        return returnValue;
    }

    protected long cacheNext(ByteBuffer in) {
        readNext(in);
        if (storedValue == GORILLA_ENCODING_ENDING_LONG) {
            hasNext = false;
        }
        return storedValue;
    }

    @SuppressWarnings("squid:S128")
    protected long readNext(ByteBuffer in) {
        byte controlBits = readNextClearBit(2, in);

        switch (controlBits) {
            case 3: // case '11': use new leading and trailing zeros
                storedLeadingZeros = (int) readLong(LEADING_ZERO_BITS_LENGTH_64BIT, in);
                byte significantBits = (byte) readLong(MEANINGFUL_XOR_BITS_LENGTH_64BIT, in);
                significantBits++;
                storedTrailingZeros = VALUE_BITS_LENGTH_64BIT - significantBits - storedLeadingZeros;
                // missing break is intentional, we want to overflow to next one
            case 2: // case '10': use stored leading and trailing zeros
                long xor = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros, in);
                xor <<= storedTrailingZeros;
                storedValue ^= xor;
                // missing break is intentional, we want to overflow to next one
            default: // case '0': use stored value
                return storedValue;
        }
    }
}
