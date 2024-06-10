package intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import lombok.Getter;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL.SHARED_LOCK;

public class CacheBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CacheBuffer.class);
    private final int workerId;
    @Getter
    private final String[] tableNames;
    @Getter
    private final ConcurrentHashMap<String, Integer> tableNameToLength = new ConcurrentHashMap<>();
    private final HashMap<String, RdmaBuffer> tableNameToRdmaBuffer = new HashMap<>();
    private final HashMap<String, ByteBuffer> tableNameToByteBuffer = new HashMap<>();//To shared among workers
    private final HashMap<String, HashMap<String, Integer>> tableNameToKeyIndexMap = new HashMap<>();//To fast update/read the value in the buffer

    private final ByteBuffer[] readBuffer;


    public CacheBuffer(int workId, IbvPd ibvPb, int length, String[] tableNames, int[] valueSize, int tthread) throws Exception {
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
        readBuffer = new ByteBuffer[tthread];
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
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < keys.size(); i ++) {
            byteBuffer.putInt(workerId);
            byteBuffer.putInt(0);//32bit ownership 32bit shared lock
            byteBuffer.put(values[i].getBytes(StandardCharsets.UTF_8));
            tableNameToKeyIndexMap.get(tableName).put(keys.get(i), i);//Key, index
        }
        byteBuffer.flip();
    }

    public String directReadCache(String tableName, String key, int threadId) throws IOException {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        int length = this.tableNameToLength.get(tableName);
        readBuffer[threadId] = tableNameToRdmaBuffer.get(tableName).getByteBufferFromOffset(index * (length + 8) + 8, length );
        byte[] valueBytes = new byte[length];
        readBuffer[threadId].get(valueBytes);
        return new String(valueBytes, StandardCharsets.UTF_8);
    }
    public void updateWriteOwnership(String tableName, String key, int workerId, int threadId) throws IOException {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        int length = this.tableNameToLength.get(tableName);
        readBuffer[threadId] = tableNameToRdmaBuffer.get(tableName).getByteBufferFromOffset(index * (length + 8), 4);
        readBuffer[threadId].order(ByteOrder.LITTLE_ENDIAN);
        readBuffer[threadId].putInt(workerId);
    }
    public void updateSharedOwnership(String tableName, String key, int threadId, int numberToRead, int biggestBid) throws IOException {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        int length = this.tableNameToLength.get(tableName);
        readBuffer[threadId] = tableNameToRdmaBuffer.get(tableName).getByteBufferFromOffset(index * (length + 8), 8);
        readBuffer[threadId].order(ByteOrder.LITTLE_ENDIAN);

        readBuffer[threadId].putInt(biggestBid);
        readBuffer[threadId].putInt(numberToRead);
    }
    public boolean checkExclusiveOwnership(String tableName, String key, int threadId) throws IOException {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        int length = this.tableNameToLength.get(tableName);
        readBuffer[threadId] = tableNameToRdmaBuffer.get(tableName).getByteBufferFromOffset(index * (length + 8),  4);
        readBuffer[threadId].order(ByteOrder.LITTLE_ENDIAN);

        int ownershipId = readBuffer[threadId].getInt();
        return ownershipId ==  workerId;
    }
    public boolean checkSharedOwnership(String tableName, String key, int threadId) throws IOException {
        int index = tableNameToKeyIndexMap.get(tableName).get(key);
        int length = this.tableNameToLength.get(tableName);
        readBuffer[threadId] = tableNameToRdmaBuffer.get(tableName).getByteBufferFromOffset(index * (length + 8),  8);
        readBuffer[threadId].order(ByteOrder.LITTLE_ENDIAN);

        int ownershipId = readBuffer[threadId].getInt();
        int numberToRead = readBuffer[threadId].getInt();
        if (numberToRead == 0) {
            readBuffer[threadId].flip();
            readBuffer[threadId].putInt(workerId);
            return true;
        } else {
            return false;
        }
    }

}
