package common.io.Encoding.encoder;

import common.io.Enums.Encoding;
import common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LongZigzagEncoder extends Encoder{
    private static final Logger logger = LoggerFactory.getLogger(LongZigzagEncoder.class);
    private List<Long> values;
    byte[] buf = new byte[10];

    public LongZigzagEncoder() {
        super(Encoding.ZIGZAG);
        this.values = new ArrayList<>();
        logger.debug("tsfile-encoding LongZigzagEncoder: long zigzag encoder");
    }

    /** encoding and bit packing */
    private byte[] encodeLong(long n) {
        n = (n << 1) ^ (n >> 63);
        int idx = 0;
        if ((n & ~0x7F) != 0) {
            buf[idx++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
            while (n > 0x7F) {
                buf[idx++] = (byte) ((n | 0x80) & 0xFF);
                n >>>= 7;
            }
        }
        buf[idx++] = (byte) n;
        return Arrays.copyOfRange(buf, 0, idx);
    }

    public static String print(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (byte b : bytes) {
            sb.append(String.format("0x%02X ", b));
        }
        sb.append("]");
        return sb.toString();
    }

    public void encode(long value, ByteArrayOutputStream out) {
        values.add(value);
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // byteCache stores all <encoded-data> and we know its size
        ByteArrayOutputStream byteCache = new ByteArrayOutputStream();
        int len = values.size();
        if (values.size() == 0) {
            return;
        }
        for (long value : values) {
            byte[] bytes = encodeLong(value);
            byteCache.write(bytes, 0, bytes.length);
        }
        // store encoded bytes size
        ReadWriteForEncodingUtils.writeUnsignedVarInt(byteCache.size(), out);
        // store initial list size
        ReadWriteForEncodingUtils.writeUnsignedVarInt(len, out);
        out.write(byteCache.toByteArray());
        reset();
    }

    private void reset() {
        values.clear();
    }

    @Override
    public long getMaxByteSize() {
        if (values == null) {
            return 0;
        }
        // try to caculate max value
        return (long) 8 + values.size() * 4;
    }
}
