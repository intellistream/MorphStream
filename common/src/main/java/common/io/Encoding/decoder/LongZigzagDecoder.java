package common.io.Encoding.decoder;

import common.io.Enums.Encoding;
import common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class LongZigzagDecoder extends Decoder{
    private static final Logger logger = LoggerFactory.getLogger(IntZigzagDecoder.class);

    /** how many bytes for all encoded data in input stream. */
    private int length;

    /** number of encoded data. */
    private int number;

    /** number of data left for reading in current buffer. */
    private int currentCount;

    /**
     * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data.
     */
    private ByteBuffer byteCache;

    public LongZigzagDecoder() {
        super(Encoding.ZIGZAG);
        this.reset();
        logger.debug("tsfile-encoding LongZigzagDecoder: long zigzag decoder");
    }

    /** decoding */
    @Override
    public long readLong(ByteBuffer buffer) {
        if (currentCount == 0) {
            reset();
            getLengthAndNumber(buffer);
            currentCount = number;
        }
        long n = 0;
        int i = 0;
        long b = 0;

        while (byteCache.hasRemaining() && ((b = byteCache.get()) & 0x80) != 0) {
            n |= (b & 0x7F) << i;
            i += 7;
        }
        n = n | (b << i);
        currentCount--;
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    private void getLengthAndNumber(ByteBuffer buffer) {
        this.length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        this.number = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        byte[] tmp = new byte[length];
        buffer.get(tmp, 0, length);
        this.byteCache = ByteBuffer.wrap(tmp);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        if (currentCount > 0 || buffer.remaining() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        this.length = 0;
        this.number = 0;
        this.currentCount = 0;
        if (this.byteCache == null) {
            this.byteCache = ByteBuffer.allocate(0);
        } else {
            this.byteCache.position(0);
        }
    }
}
