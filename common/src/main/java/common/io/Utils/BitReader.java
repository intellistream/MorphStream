package common.io.Utils;

import java.nio.ByteBuffer;

public class BitReader {
    private static final int BITS_IN_A_BYTE = 8;
    private static final byte MASKS[] = {(byte) 0xff, 0x7f, 0x3f, 0x1f, 0x0f, 0x07, 0x03, 0x01};
    private final ByteBuffer buffer;
    private int bitCnt = BITS_IN_A_BYTE;
    private byte cache = 0;

    public BitReader(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public long next(int len) {
        long ret = 0;
        while (len > 0) {
            if (bitCnt == BITS_IN_A_BYTE) {
                next();
            }
            // Number of bits read from the current byte
            int m = len + bitCnt >= BITS_IN_A_BYTE ? BITS_IN_A_BYTE - bitCnt : len;
            len -= m;
            ret = ret << m;
            byte y = (byte) (cache & MASKS[bitCnt]); // Truncate the low bits with &
            y = (byte) ((y & 0xff) >>> (BITS_IN_A_BYTE - bitCnt - m)); // Logical shift right
            ret = ret | (y & 0xff);
            bitCnt += m;
        }
        return ret;
    }

    public byte[] nextBytes(int len) {
        byte[] ret = new byte[len];
        if (bitCnt == BITS_IN_A_BYTE) {
            buffer.get(ret);
        } else {
            for (int i = 0; i < len; i++) {
                ret[i] = (byte) next(8);
            }
        }
        return ret;
    }

    public void skip() {
        this.bitCnt = BITS_IN_A_BYTE;
    }

    private void next() {
        this.cache = buffer.get();
        this.bitCnt = 0;
    }
}
