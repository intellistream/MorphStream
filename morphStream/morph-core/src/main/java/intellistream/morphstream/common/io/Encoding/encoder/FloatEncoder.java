package intellistream.morphstream.common.io.Encoding.encoder;

import intellistream.morphstream.common.io.Enums.DataType;
import intellistream.morphstream.common.io.Enums.Encoding;
import intellistream.morphstream.common.io.Exception.encoding.EncodingException;
import intellistream.morphstream.common.io.Utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encoder for float or double value using rle or two-diff according to following grammar.
 *
 * <pre>{@code
 * float encoder: <maxPointvalue> <encoded-data>
 * maxPointvalue := number for accuracy of decimal places, store as unsigned var int
 * encoded-data := same as encoder's pattern
 * }</pre>
 */
public class FloatEncoder extends Encoder {

    private final Encoder encoder;

    /**
     * number for accuracy of decimal places.
     */
    private int maxPointNumber;

    /**
     * maxPointValue = 10^(maxPointNumber).
     */
    private double maxPointValue;

    /**
     * flag to check whether maxPointNumber is saved in the stream.
     */
    private boolean isMaxPointNumberSaved;

    public FloatEncoder(Encoding encodingType, DataType dataType, int maxPointNumber) {
        super(encodingType);
        this.maxPointNumber = maxPointNumber;
        calculateMaxPonitNum();
        isMaxPointNumberSaved = false;
        if (encodingType == Encoding.RLE) {
            if (dataType == DataType.FLOAT) {
                encoder = new IntRleEncoder();
            } else if (dataType == DataType.DOUBLE) {
                encoder = new LongRleEncoder();
            } else {
                throw new EncodingException(
                        String.format("data type %s is not supported by FloatEncoder", dataType));
            }
        } else if (encodingType == Encoding.TS_2DIFF) {
            if (dataType == DataType.FLOAT) {
                encoder = new DeltaBinaryEncoder.IntDeltaEncoder();
            } else if (dataType == DataType.DOUBLE) {
                encoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            } else {
                throw new EncodingException(
                        String.format("data type %s is not supported by FloatEncoder", dataType));
            }
        } else {
            throw new EncodingException(
                    String.format("%s encoding is not supported by FloatEncoder", encodingType));
        }
    }

    @Override
    public void encode(float value, ByteArrayOutputStream out) {
        saveMaxPointNumber(out);
        int valueInt = convertFloatToInt(value);
        encoder.encode(valueInt, out);
    }

    @Override
    public void encode(double value, ByteArrayOutputStream out) {
        saveMaxPointNumber(out);
        long valueLong = convertDoubleToLong(value);
        encoder.encode(valueLong, out);
    }

    private void calculateMaxPonitNum() {
        if (maxPointNumber <= 0) {
            maxPointNumber = 0;
            maxPointValue = 1;
        } else {
            maxPointValue = Math.pow(10, maxPointNumber);
        }
    }

    private int convertFloatToInt(float value) {
        return (int) Math.round(value * maxPointValue);
    }

    private long convertDoubleToLong(double value) {
        return Math.round(value * maxPointValue);
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        encoder.flush(out);
        reset();
    }

    private void reset() {
        isMaxPointNumberSaved = false;
    }

    private void saveMaxPointNumber(ByteArrayOutputStream out) {
        if (!isMaxPointNumberSaved) {
            ReadWriteForEncodingUtils.writeUnsignedVarInt(maxPointNumber, out);
            isMaxPointNumberSaved = true;
        }
    }

    @Override
    public int getOneItemMaxSize() {
        return encoder.getOneItemMaxSize();
    }

    @Override
    public long getMaxByteSize() {
        return encoder.getMaxByteSize();
    }
}
