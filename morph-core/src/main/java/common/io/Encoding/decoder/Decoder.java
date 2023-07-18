package common.io.Encoding.decoder;


import common.io.Enums.DataType;
import common.io.Enums.Encoding;
import common.io.Exception.encoding.DecodingException;
import common.io.Utils.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public abstract class Decoder {
    private static final String ERROR_MSG = "Decoder not found: %s , DataType is : %s";
    private Encoding type;
    public Decoder(Encoding type) {
        this.type = type;
    }
    public void setType(Encoding type) {
        this.type = type;
    }
    public Encoding getType() {
        return type;
    }
    public static Decoder getDecoderByType(Encoding encoding, DataType dataType) {
        switch (encoding) {
            case PLAIN:
                return new PlainDecoder();
            case RLE:
                switch (dataType) {
                    case BOOLEAN:
                    case INT32:
                        return new IntRleDecoder();
                    case INT64:
                    case VECTOR:
                        return new LongRleDecoder();
                    case FLOAT:
                    case DOUBLE:
                        return new FloatDecoder(Encoding.valueOf(encoding.toString()), dataType);
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case TS_2DIFF:
                switch (dataType) {
                    case INT32:
                        return new DeltaBinaryDecoder.IntDeltaDecoder();
                    case INT64:
                    case VECTOR:
                        return new DeltaBinaryDecoder.LongDeltaDecoder();
                    case FLOAT:
                    case DOUBLE:
                        return new FloatDecoder(Encoding.valueOf(encoding.toString()), dataType);
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case GORILLA_V1:
                switch (dataType) {
                    case FLOAT:
                        return new SinglePrecisionDecoderV1();
                    case DOUBLE:
                        return new DoublePrecisionDecoderV1();
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case REGULAR:
                switch (dataType) {
                    case INT32:
                        return new RegularDataDecoder.IntRegularDecoder();
                    case INT64:
                    case VECTOR:
                        return new RegularDataDecoder.LongRegularDecoder();
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case GORILLA:
                switch (dataType) {
                    case FLOAT:
                        return new SinglePrecisionDecoderV2();
                    case DOUBLE:
                        return new DoublePrecisionDecoderV2();
                    case INT32:
                        return new IntGorillaDecoder();
                    case INT64:
                    case VECTOR:
                        return new LongGorillaDecoder();
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case DICTIONARY:
                return new DictionaryDecoder();
            case ZIGZAG:
                switch (dataType) {
                    case INT32:
                        return new IntZigzagDecoder();
                    case INT64:
                        return new LongZigzagDecoder();
                    default:
                        throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
                }
            case FREQ:
                return new FreqDecoder();
            default:
                throw new DecodingException(String.format(ERROR_MSG, encoding, dataType));
        }
    }
    public int readInt(ByteBuffer buffer) {
        throw new DecodingException("Method readInt is not supported by Decoder");
    }

    public boolean readBoolean(ByteBuffer buffer) {
        throw new DecodingException("Method readBoolean is not supported by Decoder");
    }

    public short readShort(ByteBuffer buffer) {
        throw new DecodingException("Method readShort is not supported by Decoder");
    }

    public long readLong(ByteBuffer buffer) {
        throw new DecodingException("Method readLong is not supported by Decoder");
    }

    public float readFloat(ByteBuffer buffer) {
        throw new DecodingException("Method readFloat is not supported by Decoder");
    }

    public double readDouble(ByteBuffer buffer) {
        throw new DecodingException("Method readDouble is not supported by Decoder");
    }

    public Binary readBinary(ByteBuffer buffer) {
        throw new DecodingException("Method readBinary is not supported by Decoder");
    }

    public BigDecimal readBigDecimal(ByteBuffer buffer) {
        throw new DecodingException("Method readBigDecimal is not supported by Decoder");
    }

    public abstract boolean hasNext(ByteBuffer buffer) throws IOException;

    public abstract void reset();
}
