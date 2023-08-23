package intellistream.morphstream.common.io.Rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaRegisteredBuffer {
    private RdmaBufferManager rdmaBufferManager = null;
    private RdmaBuffer rdmaBuffer;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private int blockOffset = 0;
    public RdmaRegisteredBuffer(RdmaBufferManager rdmaBufferManager, int length) throws IOException {
        this.rdmaBufferManager = rdmaBufferManager;
        this.rdmaBuffer = rdmaBufferManager.get(length);
        assert this.rdmaBuffer != null;
    }
    int getLkey() {
        return rdmaBuffer.getLkey();
    }
    void retain() {
        refCount.incrementAndGet();
    }
    void release() {
        int count = refCount.decrementAndGet();
        if (count <= 0) {
           free();
        }
    }
    private synchronized void free() {
        if (rdmaBuffer != null) {
            if (rdmaBufferManager != null) {
                rdmaBufferManager.put(rdmaBuffer);
            } else {
                rdmaBuffer.free();
            }
            rdmaBuffer = null;
        }
    }
    long getRegisteredAddress() {
        return rdmaBuffer.getAddress();
    }
    private int getRegisteredLength() {
        return rdmaBuffer.getLength();
    }
    ByteBuffer getByteBuffer(int length) throws IOException {
        if (blockOffset + length > getRegisteredLength()) {
         throw new IllegalArgumentException("Exceeded Registered Length!");
        }
        ByteBuffer byteBuffer;
        try {
            byteBuffer = (ByteBuffer) RdmaBuffer.directBufferConstructor.newInstance(
                    getRegisteredAddress() + blockOffset, length);
            blockOffset += length;
        } catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer expection: " + e.toString());
        }
        return byteBuffer;
    }
}

