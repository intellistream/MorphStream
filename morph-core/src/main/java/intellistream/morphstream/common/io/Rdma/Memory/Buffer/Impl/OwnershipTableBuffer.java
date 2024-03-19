package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OwnershipTableBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(OwnershipTableBuffer.class);
    private final RdmaBuffer buffer;
    private final long length;
    private long readOffset;
    private ByteBuffer ownershipTable;
    public OwnershipTableBuffer(IbvPd ibvPd, int length, int totalThreads) throws Exception {
        this.length = length;
        this.buffer = new RdmaBuffer(ibvPd, length);
        readOffset = this.buffer.getAddress();
    }

    public ByteBuffer getOwnershipTable() throws IOException {
        ownershipTable = buffer.getByteBuffer(readOffset, 6);//START_FLAG(Short) + TotalLength(Int)
        short start_flag = ownershipTable.getShort();
        if (start_flag == SOURCE_CONTROL.START_FLAG) {
            int totalLength = ownershipTable.getInt();
            ownershipTable = buffer.getByteBuffer(readOffset + 6 + totalLength, 2);
            short end_flag = ownershipTable.getShort();
            if (end_flag == SOURCE_CONTROL.END_FLAG) {
                ownershipTable = buffer.getByteBuffer(readOffset + 6, totalLength);
                readOffset += 6 + totalLength + 2;
                return ownershipTable;
            }
        }
        return null;
    }

    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }

}
