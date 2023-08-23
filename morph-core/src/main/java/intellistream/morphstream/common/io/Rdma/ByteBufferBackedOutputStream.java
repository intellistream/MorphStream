package intellistream.morphstream.common.io.Rdma;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedOutputStream extends OutputStream{
    private final ByteBuffer buf;
    public ByteBufferBackedOutputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public void write(int b) throws IOException {
        buf.put((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buf.put(b, off, len);
    }
}
