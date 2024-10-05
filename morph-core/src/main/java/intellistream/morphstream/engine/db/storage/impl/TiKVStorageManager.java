package intellistream.morphstream.engine.db.storage.impl;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TiKVStorageManager extends RemoteStorageManager{
    private static final Logger logger = LoggerFactory.getLogger(TiKVStorageManager.class);
    private TiConfiguration tiConfiguration;
    private TiSession tiSession;
    private ArrayList<RawKVClient> rawKVClientList = new ArrayList<>();
    public TiKVStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        super(cacheBuffer, totalWorker, totalThread);
        String[] pdHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.databaseHost").split(",");
        int port = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.databasePort");
        StringBuilder pdAddress = new StringBuilder();
        for (String pdHost : pdHosts) {
            pdAddress.append(pdHost).append(":").append(port).append(",");
        }
        pdAddress.deleteCharAt(pdAddress.length() - 1);
        tiConfiguration = TiConfiguration.createDefault(pdAddress.toString());
        tiConfiguration.setEnableAtomicForCAS(true);
        //tiConfiguration.setApiVersion(TiConfiguration.ApiVersion.V2);
        tiSession = TiSession.create(tiConfiguration);
        for (int i = 0; i < totalWorker; i++) {
            rawKVClientList.add(tiSession.createRawClient());
        }
    }

    @Override
    public void loadCache(DSContext context, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        for (String tableName : tableNames) {
            List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
            int interval = (int) Math.floor((double) keys.size() / totalThread);
            int start = interval * context.thisThreadId;
            int end;
            if (context.thisThreadId == totalThread - 1) {
                end = keys.size();
            } else {
                end = interval * (context.thisThreadId + 1);
            }
            List<ByteString> groupKeys = new ArrayList<>();
            HashMap<String, Integer> keyToIndex = new HashMap<>();
            for (int i = start; i < end; i++) {
                groupKeys.add(ByteString.copyFromUtf8(tableName + "_" + keys.get(i)));
                keyToIndex.put(keys.get(i), i);
            }
            processBatchGetItem(this.rawKVClientList.get(context.thisThreadId), tableName, groupKeys, keyToIndex);
            this.workerSideOwnershipTables.get(tableName).getTotalKeys().addAndGet(end - start);
        }
    }

    @Override
    public void commitCache(DSContext context, RdmaWorkerManager rdmaWorkerManager) {
        for (String tableName : tableNames) {
            List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
            int interval = (int) Math.floor((double) keys.size() / totalThread);
            int start = interval * context.thisThreadId;
            int end;
            if (context.thisThreadId == totalThread - 1) {
                end = keys.size();
            } else {
                end = interval * (context.thisThreadId + 1);
            }
            Map<ByteString, ByteString> keyValuePairs = new HashMap<>();
            for (int i = start; i < end; i++) {
                keyValuePairs.put(ByteString.copyFromUtf8(tableName + "_" + keys.get(i)), ByteString.copyFromUtf8(this.workerSideOwnershipTables.get(tableName).valueList[i]));
            }
            processBatchUpdateItems(this.rawKVClientList.get(context.thisThreadId), keyValuePairs);
        }
    }

    public void processBatchGetItem(RawKVClient client, String tableName, List<ByteString> keys, HashMap<String, Integer> keyToIndex) {
        List<Kvrpcpb.KvPair> kvPairs = client.batchGet(keys);
        for (Kvrpcpb.KvPair kvPair : kvPairs) {
            this.workerSideOwnershipTables.get(tableName).valueList[keyToIndex.get(kvPair.getKey().toStringUtf8())] =  kvPair.getValue().toStringUtf8();
        }
    }
    public void processBatchUpdateItems(RawKVClient client, Map<ByteString, ByteString> keyValuePairs) {
       client.batchPut(keyValuePairs);
    }

    @Override
    public void close() {
        for (RawKVClient rawKVClient : rawKVClientList) {
            rawKVClient.close();
        }
        try {
            tiSession.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
