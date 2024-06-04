package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.util.FixedLengthRandomString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DatabaseBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseBuffer.class);
    private final int numberItems;
    private final int itemSize;
    private final RdmaBuffer tableBuffer;
    public final String tableName;

    public DatabaseBuffer(String tableName, int numberItems, int itemSize, IbvPd ibvPd) throws Exception {
        this.numberItems = numberItems;
        this.itemSize = itemSize;
        this.tableBuffer = new RdmaBuffer(ibvPd, numberItems * (itemSize + 8));
        this.tableName = tableName;
    }
    public RegionToken createRegionToken() {
        return tableBuffer.createRegionToken();
    }

    public void initDatabaseBuffer() throws IOException {
        ByteBuffer byteBuffer = tableBuffer.getByteBuffer();
        for (int i = 0; i < numberItems; i++) {
            byteBuffer.putLong(0L);
            byteBuffer.put(FixedLengthRandomString.generateRandomFixedLengthString(itemSize).getBytes(StandardCharsets.UTF_8));
        }
        LOG.info("Table initialized with TableName: " + this.tableName + " ItemNumber: " + numberItems + " valueSize " + itemSize);
    }

}
