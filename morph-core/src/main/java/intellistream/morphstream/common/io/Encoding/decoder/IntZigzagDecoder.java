package intellistream.morphstream.common.io.Encoding.decoder;

import intellistream.morphstream.common.io.Enums.Encoding;
import intellistream.morphstream.common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class IntZigzagDecoder extends Decoder {
    private static final Logger logger = LoggerFactory.getLogger(IntZigzagDecoder.class);

    /**
     * how many bytes for all encoded data in input stream.
     */
    private int length;

    /**
     * number of encoded data.
     */
    private int number;

    /**
     * number of data left for reading in current buffer.
     */
    private int currentCount;

    /**
     * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data.
     */
    private ByteBuffer byteCache;

    public IntZigzagDecoder() {
        super(Encoding.ZIGZAG);
        this.reset();
        logger.debug("tsfile-decoding IntZigzagDecoder: int zigzag decoder");
    }

    /**
     * decoding
     */
    @Override
    public int readInt(ByteBuffer buffer) {
        if (currentCount == 0) {
            reset();
            getLengthAndNumber(buffer);
            currentCount = number;
        }
        int n = ReadWriteForEncodingUtils.readUnsignedVarInt(byteCache);
        currentCount--;
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    private void getLengthAndNumber(ByteBuffer buffer) {
        this.length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        this.number = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        // TODO maybe this.byteCache = buffer is faster, but not safe
        byte[] tmp = new byte[length];
        buffer.get(tmp, 0, length);
        this.byteCache = ByteBuffer.wrap(tmp);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        return currentCount > 0 || buffer.remaining() > 0;
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
