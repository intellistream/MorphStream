package intellistream.morphstream.common.io.Rdma.Memory;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CircularRdmaBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CircularRdmaBuffer.class);
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
    public Tuple2<Long, ByteBuffer> canRead(int threadId) throws IOException {
        long baseOffset = 0L;
        canRead[threadId] = this.buffer.getByteBuffer(readOffset[threadId], 6);//START_FLAG(short) + TotalLength(Int)
        short start_flag = canRead[threadId].getShort();
        if (start_flag == SOURCE_CONTROL.START_FLAG) {
            int totalLength = canRead[threadId].getInt();
            canRead[threadId] = this.buffer.getByteBuffer(readOffset[threadId] + 6 + 4L * totalThreads + totalLength, totalLength);
            short end_flag = canRead[threadId].getShort();
            if (end_flag == SOURCE_CONTROL.END_FLAG) {
                baseOffset = readOffset[threadId] + 6 + 4L * totalThreads;//Message Start
                canRead[threadId] = this.buffer.getByteBuffer(readOffset[threadId] + 6, 4 * totalThreads);
                readOffset[threadId] = readOffset[threadId] + 2L + 4L + 4L * totalThreads + totalLength + 2L;
            } else {
                LOG.info("This buffer is not complete!");
            }
        }
        canRead[threadId].flip();
        return new Tuple2<>(baseOffset, canRead[threadId]);
    }
    public ByteBuffer read(long address, int length) throws IOException {
        return this.buffer.getByteBuffer(address, length);
    }

    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }
}
