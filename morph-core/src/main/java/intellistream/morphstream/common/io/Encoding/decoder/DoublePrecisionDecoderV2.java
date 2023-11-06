package intellistream.morphstream.common.io.Encoding.decoder;

import java.nio.ByteBuffer;

import static intellistream.morphstream.common.io.Utils.FileConfig.GORILLA_ENCODING_ENDING_DOUBLE;

public class DoublePrecisionDecoderV2 extends LongGorillaDecoder {

    private static final long GORILLA_ENCODING_ENDING =
            Double.doubleToRawLongBits(GORILLA_ENCODING_ENDING_DOUBLE);

    @Override
    public final double readDouble(ByteBuffer in) {
        return Double.longBitsToDouble(readLong(in));
    }

    @Override
    protected long cacheNext(ByteBuffer in) {
        readNext(in);
        if (storedValue == GORILLA_ENCODING_ENDING) {
            hasNext = false;
        }
        return storedValue;
    }
}
