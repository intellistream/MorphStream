package intellistream.morphstream.common.io.Rdma.Memory.Buffer;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(TableBuffer.class);
    private final RdmaBuffer buffer;
    private final long length;
    public TableBuffer(IbvPd ibvPd, int length) throws Exception {
        this.length = length;
        this.buffer = new RdmaBuffer(ibvPd, length);
    }
    public RegionToken createRegionToken() {
        return buffer.createRegionToken();
    }

}
