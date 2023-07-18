package common.io.Encoding.decoder;

import java.nio.ByteBuffer;

import static common.io.Utils.FileConfig.GORILLA_ENCODING_ENDING_FLOAT;

public class SinglePrecisionDecoderV2 extends IntGorillaDecoder {

    private static final int GORILLA_ENCODING_ENDING =
            Float.floatToRawIntBits(GORILLA_ENCODING_ENDING_FLOAT);

    @Override
    public final float readFloat(ByteBuffer in) {
        return Float.intBitsToFloat(readInt(in));
    }

    @Override
    protected int cacheNext(ByteBuffer in) {
        readNext(in);
        if (storedValue == GORILLA_ENCODING_ENDING) {
            hasNext = false;
        }
        return storedValue;
    }
}
