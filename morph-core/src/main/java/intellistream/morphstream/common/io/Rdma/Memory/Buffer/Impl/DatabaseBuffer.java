package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
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
    public final String tempValueForTables;
    public final String tableName;

    public DatabaseBuffer(String tableName, int numberItems, int itemSize, IbvPd ibvPd) throws Exception {
        this.numberItems = numberItems;
        this.itemSize = itemSize;
        this.tableBuffer = new RdmaBuffer(ibvPd, (numberItems + 2) * itemSize);
        this.tableName = tableName;
        this.tempValueForTables = padStringToLength(tableName, itemSize);
    }
    public RegionToken createRegionToken() {
        return tableBuffer.createRegionToken();
    }

    public void initDatabaseBuffer() throws IOException {
        ByteBuffer byteBuffer = tableBuffer.getByteBuffer();
        for (int i = 0; i < numberItems; i++) {
            byteBuffer.putShort((short) 0);
            byteBuffer.put(tempValueForTables.getBytes(StandardCharsets.UTF_8));
        }
        LOG.info("Table initialized with TableName: " + this.tableName + " ItemNumber: " + numberItems + " valueSize " + itemSize);
    }
    private String padStringToLength(String str, int length) {
        if (str.length() >= length) {
            return str.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() < length) {
            sb.append(' ');
        }
        return sb.toString();
    }

}
