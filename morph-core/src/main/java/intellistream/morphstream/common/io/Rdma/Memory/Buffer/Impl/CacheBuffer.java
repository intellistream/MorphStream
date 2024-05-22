package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import lombok.Getter;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CacheBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CacheBuffer.class);
    private int workerId;
    @Getter
    private final String[] tableNames;
    @Getter
    private final ConcurrentHashMap<String, Integer> tableNameToLength = new ConcurrentHashMap<>();
    private final HashMap<String, RdmaBuffer> tableNameToRdmaBuffer = new HashMap<>();
    private final HashMap<String, ByteBuffer> tableNameToByteBuffer = new HashMap<>();//To shared among workers
    private final HashMap<String, HashMap<String, Integer>> tableNameToKeyIndexMap = new HashMap<>();//To fast update/read the value in the buffer


    public CacheBuffer(int workId, IbvPd ibvPb, int length, String[] tableNames, int[] valueSize) throws Exception {
        this.workerId = workId;
        for (String tableName : tableNames) {
            tableNameToRdmaBuffer.put(tableName, new RdmaBuffer(ibvPb, length));
            tableNameToKeyIndexMap.put(tableName, new HashMap<>());
            tableNameToByteBuffer.put(tableName, tableNameToRdmaBuffer.get(tableName).getByteBuffer());
        }
        this.tableNames = tableNames;
        for (int i = 0; i < tableNames.length; i++) {
            tableNameToLength.put(tableNames[i], valueSize[i]);
        }
    }
    public List<RegionToken> createRegionTokens() {
        List<RegionToken> regionTokens = new ArrayList<>();
        for (String tableName : tableNames) {
            regionTokens.add(tableNameToRdmaBuffer.get(tableName).createRegionToken());
        }
        return regionTokens;
    }
    public void initLocalCacheBuffer(List<String> keys, String[] values, String tableName) throws IOException {
        LOG.info("The number of ownership keys is " + keys.size());
        ByteBuffer byteBuffer = tableNameToRdmaBuffer.get(tableName).getByteBuffer();

        for (int i = 0; i < keys.size(); i ++) {
            byteBuffer.putShort((short) workerId);//OwnershipId (Short) 2
            byteBuffer.put(values[i].getBytes(StandardCharsets.UTF_8));
            tableNameToKeyIndexMap.get(tableName).put(keys.get(i), i);//Key, index
        }
        byteBuffer.flip();
    }

    public synchronized String readCache(String tableName, String key, int workerId) {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        tableNameToByteBuffer.get(tableName).position(index * (this.tableNameToLength.get(tableName) + 2)); // 移动到指定位置
        short ownershipId = tableNameToByteBuffer.get(tableName).getShort();
        if (ownershipId == workerId) {
            int length = this.tableNameToLength.get(tableName);
            byte[] valueBytes = new byte[length];
            tableNameToByteBuffer.get(tableName).get(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        } else {
            return "false";
        }
    }
    public synchronized void updateOwnership(String tableName, String key, int workerId) {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        tableNameToByteBuffer.get(tableName).position(index * (this.tableNameToLength.get(tableName) + 2));
        tableNameToByteBuffer.get(tableName).putShort((short) workerId);
    }
    public synchronized boolean checkOwnership(String tableName, String key) {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        tableNameToByteBuffer.get(tableName).position(index * (this.tableNameToLength.get(tableName) + 2));
        short ownershipId = tableNameToByteBuffer.get(tableName).getShort();
        return ownershipId == (short) workerId;
    }

}
