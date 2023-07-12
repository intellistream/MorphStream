package common.io.Encoding.decoder;

import common.io.Enums.Encoding;
import common.io.Exception.encoding.DecodingException;
import common.io.Utils.Binary;
import common.io.Utils.ReadWriteForEncodingUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class PlainDecoder extends Decoder {

    public PlainDecoder() {
        super(Encoding.PLAIN);
    }

    @Override
    public boolean readBoolean(ByteBuffer buffer) {
        return buffer.get() != 0;
    }

    @Override
    public short readShort(ByteBuffer buffer) {
        return buffer.getShort();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
        return ReadWriteForEncodingUtils.readVarInt(buffer);
    }

    @Override
    public long readLong(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public float readFloat(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public double readDouble(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        int length = readInt(buffer);
        byte[] buf = new byte[length];
        buffer.get(buf, 0, buf.length);
        return new Binary(buf);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        return buffer.remaining() > 0;
    }

    @Override
    public BigDecimal readBigDecimal(ByteBuffer buffer) {
        throw new DecodingException("Method readBigDecimal is not supported by PlainDecoder");
    }

    @Override
    public void reset() {
        // do nothing
    }
}

