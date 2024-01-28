package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CacheBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CacheBuffer.class);
    private final HashMap<String, RdmaBuffer> tableNameToBuffer = new HashMap<>();
    private final long length;
    public CacheBuffer(IbvPd ibvPb, int length, String[] tableNames) throws Exception {
        this.length = length;
        for (String tableName : tableNames) {
            tableNameToBuffer.put(tableName, new RdmaBuffer(ibvPb, length));
        }
    }
    public List<RegionToken> createRegionTokens() {
        List<RegionToken> regionTokens = new ArrayList<>();
        for (RdmaBuffer buffer : tableNameToBuffer.values()) {
            regionTokens.add(buffer.createRegionToken());
        }
        return regionTokens;
    }

}
