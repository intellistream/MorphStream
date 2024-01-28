package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TableBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(TableBuffer.class);
    private final RdmaBuffer buffer;
    private final long length;
    private final long[] readOffset;
    private final ByteBuffer[] ownershipTable;
    public TableBuffer(IbvPd ibvPd, int length, int totalThreads) throws Exception {
        this.length = length;
        this.buffer = new RdmaBuffer(ibvPd, length);
        this.readOffset = new long[totalThreads];
        this.ownershipTable = new ByteBuffer[totalThreads];
        for (int i = 0; i < totalThreads; i++) {
            readOffset[i] = this.buffer.getAddress();
        }
    }

    public ByteBuffer getOwnershipTable(int threadId) throws IOException {
        ownershipTable[threadId] = buffer.getByteBuffer(readOffset[threadId], 6);//START_FLAG(Short) + TotalLength(Int)
        short start_flag = ownershipTable[threadId].getShort();
        if (start_flag == SOURCE_CONTROL.START_FLAG) {
            int totalLength = ownershipTable[threadId].getInt();
            ownershipTable[threadId] = buffer.getByteBuffer(readOffset[threadId] + 6 + totalLength, 2);
            short end_flag = ownershipTable[threadId].getShort();
            if (end_flag == SOURCE_CONTROL.END_FLAG) {
                ownershipTable[threadId] = buffer.getByteBuffer(readOffset[threadId] + 6, totalLength);
                readOffset[threadId] += 6 + totalLength + 2;
                return ownershipTable[threadId];
            }
        }
        return null;
    }

    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }

}
