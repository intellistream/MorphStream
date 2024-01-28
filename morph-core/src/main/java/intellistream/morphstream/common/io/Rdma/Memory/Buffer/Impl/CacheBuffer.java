package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class CacheBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CacheBuffer.class);
    private final HashMap<String, RdmaBuffer> tableNameToRdmaBuffer = new HashMap<>();
    private final HashMap<String, ByteBuffer> tableNameToByteBuffer = new HashMap<>();
    private final HashMap<String, HashMap<String, Integer>> tableNameToKeyIndexMap = new HashMap<>();

    public CacheBuffer(IbvPd ibvPb, int length, String[] tableNames) throws Exception {
        for (String tableName : tableNames) {
            tableNameToRdmaBuffer.put(tableName, new RdmaBuffer(ibvPb, length));
            tableNameToKeyIndexMap.put(tableName, new HashMap<>());
            tableNameToByteBuffer.put(tableName, tableNameToRdmaBuffer.get(tableName).getByteBuffer());
        }
    }
    public List<RegionToken> createRegionTokens() {
        List<RegionToken> regionTokens = new ArrayList<>();
        for (RdmaBuffer buffer : tableNameToRdmaBuffer.values()) {
            regionTokens.add(buffer.createRegionToken());
        }
        return regionTokens;
    }
    public void initLocalCacheBuffer(HashMap<String, Integer> keyValues, String tableName) throws IOException {
        List<String> tempSortList = new ArrayList<>(keyValues.keySet());
        Collections.sort(tempSortList);
        ByteBuffer byteBuffer = tableNameToRdmaBuffer.get(tableName).getByteBuffer();

        for (int i = 0; i < tempSortList.size(); i++) {
            byteBuffer.putInt(keyValues.get(tempSortList.get(i)));
            tableNameToKeyIndexMap.get(tableName).put(tempSortList.get(i), i);
        }
        byteBuffer.flip();
    }
    public int readCache(String tableName, String key, int workerId) {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        tableNameToByteBuffer.get(tableName).position(index * 8);
        if (tableNameToByteBuffer.get(tableName).getInt() == workerId) {
            return tableNameToByteBuffer.get(tableName).getInt();
        } else {
            return -1;
        }
    }

}
