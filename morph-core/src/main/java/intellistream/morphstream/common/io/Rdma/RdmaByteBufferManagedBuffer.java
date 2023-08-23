package intellistream.morphstream.common.io.Rdma;

import com.google.common.base.Objects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class RdmaByteBufferManagedBuffer extends RdmaManagedBuffer{
    private final RdmaRegisteredBuffer rdmaRegisteredBuffer;
    private ByteBuffer byteBuffer;
    public RdmaByteBufferManagedBuffer(RdmaRegisteredBuffer rdmaRegisteredBuffer, int length) throws IOException {
        this.rdmaRegisteredBuffer = rdmaRegisteredBuffer;
        this.byteBuffer = rdmaRegisteredBuffer.getByteBuffer(length);
        this.byteBuffer.limit(length);
        retain();
    }
    @Override
    public long size() {
        return byteBuffer.remaining();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return byteBuffer;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufferBackedInputStream(byteBuffer);
    }

    public OutputStream createOutputStream() throws IOException {
        return new ByteBufferBackedOutputStream(byteBuffer);
    }

    @Override
    public RdmaManagedBuffer retain() {
        rdmaRegisteredBuffer.retain();
        return this;
    }

    @Override
    public RdmaManagedBuffer release() {
        rdmaRegisteredBuffer.release();
        return this;
    }

    @Override
    public long getAddress() {
        return ((sun.nio.ch.DirectBuffer)byteBuffer).address();
    }

    @Override
    public int getLkey() {
        return rdmaRegisteredBuffer.getLkey();
    }

    @Override
    public long getLength() {
        return byteBuffer.capacity();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("rdmaRegisteredBuffer", rdmaRegisteredBuffer)
                .toString();
    }
}
