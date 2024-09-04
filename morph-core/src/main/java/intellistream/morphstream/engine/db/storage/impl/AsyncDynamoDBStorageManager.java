package intellistream.morphstream.engine.db.storage.impl;

import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AsyncDynamoDBStorageManager extends RemoteStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncDynamoDBStorageManager.class);
    private DynamoDbAsyncClient dynamoDBAsyncClient;

    public AsyncDynamoDBStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        super(cacheBuffer, totalWorker, totalThread);
        dynamoDBAsyncClient = DynamoDbAsyncClient.builder().region(Region.US_EAST_2).build();
    }

    @Override
    public void loadCache(DSContext context, RdmaWorkerManager rdmaWorkerManager) throws Exception {
        for (String tableName : tableNames) {
            List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
            int interval = (int) Math.floor((double) keys.size() / totalThread);
            int start = interval * context.thisThreadId;
            int end = (context.thisThreadId == totalThread - 1) ? keys.size() : interval * (context.thisThreadId + 1);

            List<Map<String, AttributeValue>> groupKeys = new ArrayList<>();
            for (int i = start; i < end; i++) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("id", AttributeValue.builder().s(keys.get(i)).build());
                groupKeys.add(key);
            }

            // 异步批量获取
            processBatchGetItemAsync(dynamoDBAsyncClient, tableName, groupKeys, start, end);
        }
    }

    @Override
    public void commitCache(DSContext context, RdmaWorkerManager rdmaWorkerManager) {
        for (String tableName : tableNames) {
            List<String> keys = this.workerSideOwnershipTables.get(tableName).getKeysForThisWorker();
            int interval = (int) Math.floor((double) keys.size() / totalThread);
            int start = interval * context.thisThreadId;
            int end = (context.thisThreadId == totalThread - 1) ? keys.size() : interval * (context.thisThreadId + 1);

            List<Map<String, AttributeValue>> batchKeys = new ArrayList<>();
            List<Map<String, AttributeValueUpdate>> batchUpdates = new ArrayList<>();
            for (int i = start; i < end; i++) {
                String key = keys.get(i);
                Map<String, AttributeValue> keyMap = new HashMap<>();
                keyMap.put("id", AttributeValue.builder().s(key).build());

                Map<String, AttributeValueUpdate> updates = new HashMap<>();
                updates.put(this.tableNameToValueName.get(tableName), AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s(this.workerSideOwnershipTables.get(tableName).valueList[i]).build())
                        .action(AttributeAction.PUT)
                        .build());

                batchKeys.add(keyMap);
                batchUpdates.add(updates);
            }

            // 异步批量更新
            processBatchUpdateItemAsync(dynamoDBAsyncClient, tableName, batchKeys, batchUpdates);
        }
    }

    private void processBatchGetItemAsync(DynamoDbAsyncClient dynamoDBAsyncClient, String tableName, List<Map<String, AttributeValue>> batchKeys, int start, int end) {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put(tableName, KeysAndAttributes.builder().keys(batchKeys).build());

        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();

        // 异步执行批量获取操作
        dynamoDBAsyncClient.batchGetItem(batchGetItemRequest).thenAccept(response -> {
            Map<String, List<Map<String, AttributeValue>>> responses = response.responses();

            // 本地更新 start
            int updatedStart = start;

            // 处理 DynamoDB 的响应数据
            if (responses.containsKey(tableName)) {
                for (Map<String, AttributeValue> item : responses.get(tableName)) {
                    this.workerSideOwnershipTables.get(tableName).valueList[updatedStart] = item.get(this.tableNameToValueName.get(tableName)).s();
                    updatedStart ++;  // 更新 start，确保在下一次递归调用中不会覆盖已处理的值
                }
            }

            // 检查未处理的项目并递归重试
            Map<String, KeysAndAttributes> unprocessedKeys = response.unprocessedKeys();
            if (!unprocessedKeys.isEmpty()) {
                LOG.info("Retrying unprocessed keys for table: " + tableName);

                // 递归调用自身，传递更新后的 start 索引
                List<Map<String, AttributeValue>> unprocessedBatchKeys = new ArrayList<>(batchKeys.subList(updatedStart - start, batchKeys.size()));
                processBatchGetItemAsync(dynamoDBAsyncClient, tableName, unprocessedBatchKeys, updatedStart, end);
            }
        }).exceptionally(e -> {
            LOG.error("Failed to get items asynchronously: " + e.getMessage());
            return null;
        });
    }



    private void processBatchUpdateItemAsync(DynamoDbAsyncClient dynamoDBAsyncClient, String tableName, List<Map<String, AttributeValue>> batchKeys, List<Map<String, AttributeValueUpdate>> batchUpdates) {
        List<CompletableFuture<UpdateItemResponse>> futures = new ArrayList<>();

        // 遍历每个键及其对应的更新操作
        for (int i = 0; i < batchKeys.size(); i++) {
            Map<String, AttributeValue> key = batchKeys.get(i);  // 键（例如 UserID）
            Map<String, AttributeValueUpdate> updates = batchUpdates.get(i);  // 更新内容

            // 创建 UpdateItemRequest
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .attributeUpdates(updates)
                    .build();

            // 异步执行更新
            CompletableFuture<UpdateItemResponse> future = dynamoDBAsyncClient.updateItem(updateItemRequest);
            futures.add(future);

            future.thenAccept(response -> {
                LOG.info("Successfully updated item with key: " + key.get("id").s());
            }).exceptionally(e -> {
                LOG.error("Failed to update item with key: " + key.get("id").s() + " - " + e.getMessage());
                return null;
            });
        }

        // 等待所有更新完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        LOG.info("All async updates completed for table: " + tableName);
    }
}
