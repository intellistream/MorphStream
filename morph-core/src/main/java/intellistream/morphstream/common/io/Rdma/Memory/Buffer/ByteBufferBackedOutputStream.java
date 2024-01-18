package intellistream.morphstream.common.io.Rdma.Memory.Buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedOutputStream extends OutputStream {
    private final ByteBuffer buf;
    public ByteBufferBackedOutputStream(ByteBuffer buf) {
        this.buf = buf;
    }
    @Override
    public void write(int b) throws IOException {
        buf.put((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        buf.put(b);
    }

    public void write(byte[] bytes, int off, int len)
            throws IOException {
        buf.put(bytes, off, len);
    }

    public ByteBuffer buffer() {
        return buf;
    }

    public void writeShort(short size) {
        buf.putShort(size);
    }

    public void writeInt(int encodeLength) {
        buf.putInt(encodeLength);
    }
}
