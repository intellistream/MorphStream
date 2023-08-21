package common.io.Encoding.decoder;

import common.io.Enums.DataType;
import common.io.Enums.Encoding;
import common.io.Exception.encoding.DecodingException;
import common.io.Utils.Binary;
import common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for float or double value using rle or two diff. For more info about encoding pattern,
 * see{@link common.io.Encoding.encoder.FloatEncoder}
 */
public class FloatDecoder extends Decoder {

    private static final Logger logger = LoggerFactory.getLogger(FloatDecoder.class);
    private Decoder decoder;

    /** maxPointValue = 10^(maxPointNumber). maxPointNumber can be read from the stream. */
    private double maxPointValue;

    /** flag that indicates whether we have read maxPointNumber and calculated maxPointValue. */
    private boolean isMaxPointNumberRead;

    public FloatDecoder(Encoding encodingType, DataType dataType) {
        super(encodingType);
        if (encodingType == Encoding.RLE) {
            if (dataType == DataType.FLOAT) {
                decoder = new IntRleDecoder();
                logger.debug("tsfile-encoding FloatDecoder: init decoder using int-rle and float");
            } else if (dataType == DataType.DOUBLE) {
                decoder = new LongRleDecoder();
                logger.debug("tsfile-encoding FloatDecoder: init decoder using long-rle and double");
            } else {
                throw new DecodingException(
                        String.format("data type %s is not supported by FloatDecoder", dataType));
            }
        } else if (encodingType == Encoding.TS_2DIFF) {
            if (dataType == DataType.FLOAT) {
                decoder = new DeltaBinaryDecoder.IntDeltaDecoder();
                logger.debug("tsfile-encoding FloatDecoder: init decoder using int-delta and float");
            } else if (dataType == DataType.DOUBLE) {
                decoder = new DeltaBinaryDecoder.LongDeltaDecoder();
                logger.debug("tsfile-encoding FloatDecoder: init decoder using long-delta and double");
            } else {
                throw new DecodingException(
                        String.format("data type %s is not supported by FloatDecoder", dataType));
            }
        } else {
            throw new DecodingException(
                    String.format("%s encoding is not supported by FloatDecoder", encodingType));
        }
        isMaxPointNumberRead = false;
    }

    @Override
    public float readFloat(ByteBuffer buffer) {
        readMaxPointValue(buffer);
        int value = decoder.readInt(buffer);
        double result = value / maxPointValue;
        return (float) result;
    }

    @Override
    public double readDouble(ByteBuffer buffer) {
        readMaxPointValue(buffer);
        long value = decoder.readLong(buffer);
        return value / maxPointValue;
    }

    private void readMaxPointValue(ByteBuffer buffer) {
        if (!isMaxPointNumberRead) {
            int maxPointNumber = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
            if (maxPointNumber <= 0) {
                maxPointValue = 1;
            } else {
                maxPointValue = Math.pow(10, maxPointNumber);
            }
            isMaxPointNumberRead = true;
        }
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        if (decoder == null) {
            return false;
        }
        return decoder.hasNext(buffer);
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        throw new DecodingException("Method readBinary is not supported by FloatDecoder");
    }

    @Override
    public boolean readBoolean(ByteBuffer buffer) {
        throw new DecodingException("Method readBoolean is not supported by FloatDecoder");
    }

    @Override
    public short readShort(ByteBuffer buffer) {
        throw new DecodingException("Method readShort is not supported by FloatDecoder");
    }

    @Override
    public int readInt(ByteBuffer buffer) {
        throw new DecodingException("Method readInt is not supported by FloatDecoder");
    }

    @Override
    public long readLong(ByteBuffer buffer) {
        throw new DecodingException("Method readLong is not supported by FloatDecoder");
    }

    @Override
    public void reset() {
        this.decoder.reset();
        this.isMaxPointNumberRead = false;
    }
}
