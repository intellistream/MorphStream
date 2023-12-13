package intellistream.morphstream.common.io.Rdma.Memory;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CircularRdmaBuffer {
    private final RdmaBuffer buffer;
    private final long length;
    private final int totalThreads;

    private long readOffset;
    public CircularRdmaBuffer(IbvPd ibvPd, int length, int totalThreads) throws Exception {
        this.length = length;
        this.buffer = new RdmaBuffer(ibvPd, length);
        this.readOffset = this.buffer.getAddress();
        this.totalThreads = totalThreads;
    }
    public ByteBuffer canRead() throws IOException {
        ByteBuffer byteBuffer = this.buffer.getByteBuffer(readOffset, 4 * totalThreads);
        int length = byteBuffer.getInt();
        readOffset = readOffset + length;
        if (length != 0) {
           while (byteBuffer.hasRemaining()) {
               readOffset = readOffset + byteBuffer.getInt();
           }
           readOffset = readOffset + 4 * totalThreads;
        }
        return this.buffer.getByteBuffer(readOffset, 4 * totalThreads);
    }
    public ByteBuffer read(long address, int length) throws IOException {
        return this.buffer.getByteBuffer(address, length);
    }

    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }
}
