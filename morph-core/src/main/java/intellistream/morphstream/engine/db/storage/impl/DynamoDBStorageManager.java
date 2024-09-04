package intellistream.morphstream.engine.db.storage.impl;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBStorageManager extends RemoteStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStorageManager.class);
    private DynamoDbClient dynamoDBClient;

    public DynamoDBStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        super(cacheBuffer, totalWorker, totalThread);
        dynamoDBClient = DynamoDbClient.builder().region(Region.US_EAST_2).build();
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
            List<Map<String, AttributeValue>> groupKeys = new ArrayList<>();
            for (int i = start; i < end; i++) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("id", AttributeValue.builder().s(keys.get(i)).build());
                groupKeys.add(key);
            }
            processBatchGetItem(dynamoDBClient, tableName, groupKeys, start, end);
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
            List<Map<String, AttributeValue>> batchKeys = new ArrayList<>();
            List<Map<String, AttributeValueUpdate>> batchUpdates = new ArrayList<>();
            for (int i = start; i < end; i++) {
                String key = keys.get(i);
                Map<String, AttributeValue> keyMap = new HashMap<>();
                keyMap.put("id", AttributeValue.builder().s(key).build());
                Map<String, AttributeValueUpdate> updates = new HashMap<>();
                updates.put(this.tableNameToValueName.get(tableName), AttributeValueUpdate.builder().value(AttributeValue.builder().s(this.workerSideOwnershipTables.get(tableName).valueList[i]).build()).action(AttributeAction.PUT).build());
                batchKeys.add(keyMap);
                batchUpdates.add(updates);
            }
            processBatchUpdateItem(dynamoDBClient, tableName, batchKeys, batchUpdates);
        }
    }

    private void processBatchGetItem(DynamoDbClient dynamicDBClient, String tableName, List<Map<String, AttributeValue>> batchKeys, int start, int end) {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put(tableName, KeysAndAttributes.builder().keys(batchKeys).build());

        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();
        BatchGetItemResponse response;

        do {
            // 执行批量获取操作
            response = dynamicDBClient.batchGetItem(batchGetItemRequest);

            // 处理响应中的数据
            Map<String, List<Map<String, AttributeValue>>> responses = response.responses();
            for (Map<String, AttributeValue> item : responses.get(tableName)) {
                // 存储获取的值
                this.workerSideOwnershipTables.get(tableName).valueList[start] = item.get(this.tableNameToValueName.get(tableName)).s();
                start++;  // 递增 start
            }

            // 处理未处理的项目
            requestItems = response.unprocessedKeys();
            if (!requestItems.isEmpty()) {
                batchGetItemRequest = BatchGetItemRequest.builder()
                        .requestItems(requestItems)
                        .build();
            }
        } while (!requestItems.isEmpty());  // 如果有未处理的项目，则继续重试
    }
    private void processBatchUpdateItem(DynamoDbClient dynamoDBClient, String tableName, List<Map<String, AttributeValue>> batchKeys, List<Map<String, AttributeValueUpdate>> batchUpdates) {
        // Iterate over the keys and their corresponding updates
        for (int i = 0; i < batchKeys.size(); i++) {
            Map<String, AttributeValue> key = batchKeys.get(i);  // The key (UserID)
            Map<String, AttributeValueUpdate> updates = batchUpdates.get(i);  // Updates for other fields (e.g., PWD)

            // Build UpdateItemRequest
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .attributeUpdates(updates)
                    .build();

            try {
                // Execute the update
                dynamoDBClient.updateItem(updateItemRequest);
            } catch (DynamoDbException e) {
                LOG.error("Failed to update item with key: " + key.get("UserID").s() + " - " + e.getMessage());
            }
        }
    }


}
