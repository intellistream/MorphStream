package intellistream.morphstream.common.io.Rdma.Memory;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CircularRdmaBuffer {
    private final RdmaBuffer buffer;
    private final long length;
    private final int totalThreads;
    private long[] readOffset;
    private ByteBuffer[] canRead;
    public CircularRdmaBuffer(IbvPd ibvPd, int length, int totalThreads) throws Exception {
        this.length = length;
        this.buffer = new RdmaBuffer(ibvPd, length);
        this.readOffset = new long[totalThreads];
        this.canRead = new ByteBuffer[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
            readOffset[i] = this.buffer.getAddress();
        }
        this.totalThreads = totalThreads;
    }
    public ByteBuffer canRead(int threadId) throws IOException {
        canRead[threadId] = this.buffer.getByteBuffer(readOffset[threadId], 4 * totalThreads);
        int length = canRead[threadId].getInt();
        if (length != 0) {
           while (canRead[threadId].hasRemaining()) {
               readOffset[threadId] = readOffset[threadId] + canRead[threadId].getInt();
           }
            readOffset[threadId] = readOffset[threadId] + 4L * totalThreads;
        }
        canRead[threadId].flip();
        return canRead[threadId];
    }
    public ByteBuffer read(long address, int length) throws IOException {
        return this.buffer.getByteBuffer(address, length);
    }

    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }
}
