package intellistream.morphstream.engine.db.storage.impl;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DynamoDBStorageManager extends RemoteStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStorageManager.class);
    private DynamoDbClient dynamoDBClient;
    private DynamoDbAsyncClient dynamoDBAsyncClient;

    public DynamoDBStorageManager(CacheBuffer cacheBuffer, int totalWorker, int totalThread) {
        super(cacheBuffer, totalWorker, totalThread);
        dynamoDBClient = DynamoDbClient.builder().region(Region.US_EAST_2).build();
        dynamoDBAsyncClient = DynamoDbAsyncClient.builder().region(Region.US_EAST_2).build();
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
            HashMap<String, Integer> keyToIndex = new HashMap<>();
            for (int i = start; i < end; i++) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("id", AttributeValue.builder().s(keys.get(i)).build());
                groupKeys.add(key);
                keyToIndex.put(keys.get(i), i);
            }
            processBatchGetItem(dynamoDBClient, tableName, groupKeys, keyToIndex);
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
            List<String> newValues = new ArrayList<>();
            for (int i = start; i < end; i++) {
                String key = keys.get(i);
                Map<String, AttributeValue> keyMap = new HashMap<>();
                keyMap.put("id", AttributeValue.builder().s(key).build());
                batchKeys.add(keyMap);
                newValues.add(this.workerSideOwnershipTables.get(tableName).valueList[i]);
            }
            processBatchUpdateItemsAsync(dynamoDBAsyncClient, tableName, batchKeys, this.tableNameToValueName.get(tableName), newValues);
        }
    }

    private void processBatchGetItem(DynamoDbClient dynamicDBClient, String tableName, List<Map<String, AttributeValue>> batchKeys, HashMap<String, Integer> keyToIndex) {
        // 每次处理100个items
        int batchSize = 100;
        int totalItems = batchKeys.size();

        // 分批处理batchKeys
        for (int i = 0; i < totalItems; i += batchSize) {
            int end = Math.min(i + batchSize, totalItems);
            List<Map<String, AttributeValue>> currentBatchKeys = batchKeys.subList(i, end);

            // 构建requestItems
            Map<String, KeysAndAttributes> requestItems = new HashMap<>();
            requestItems.put(tableName, KeysAndAttributes.builder().keys(currentBatchKeys).build());

            BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();
            BatchGetItemResponse response;

            do {
                // 执行批量获取操作
                response = dynamicDBClient.batchGetItem(batchGetItemRequest);

                // 处理响应中的数据
                Map<String, List<Map<String, AttributeValue>>> responses = response.responses();
                for (Map<String, AttributeValue> item : responses.get(tableName)) {
                    // 存储获取的值
                    this.workerSideOwnershipTables.get(tableName).valueList[keyToIndex.get(item.get("id").s())] = item.get(this.tableNameToValueName.get(tableName)).s();
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
    }

    public CompletableFuture<Void> processBatchUpdateItemsAsync(DynamoDbAsyncClient dynamoDbAsyncClient,
                                                                String tableName,
                                                                List<Map<String, AttributeValue>> batchKeys,
                                                                String valueName,
                                                                List<String> newValues) {
        // 验证输入数据
        if (batchKeys.size() != newValues.size()) {
            throw new IllegalArgumentException("The number of keys and values must be the same");
        }

        // 异步更新所有项目
        List<CompletableFuture<UpdateItemResponse>> updateFutures = batchKeys.stream()
                .map(key -> {
                    int index = batchKeys.indexOf(key);
                    String newValue = newValues.get(index);
                    return updateItemAsync(dynamoDbAsyncClient, tableName, key, valueName, newValue);
                })
                .collect(Collectors.toList());

        // 等待所有异步操作完成
        return CompletableFuture.allOf(updateFutures.toArray(new CompletableFuture[0]));
    }
    public CompletableFuture<UpdateItemResponse> updateItemAsync(DynamoDbAsyncClient dynamoDbAsyncClient,
                                                                 String tableName,
                                                                 Map<String, AttributeValue> key,
                                                                 String valueName,
                                                                 String newValue) {

        // 构建更新表达式
        String updateExpression = "SET " + valueName + " = :newValue";

        // 定义表达式中的值
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":newValue", AttributeValue.builder().s(newValue).build());

        // 构建更新请求
        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression(updateExpression)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        // 异步执行更新操作
        return dynamoDbAsyncClient.updateItem(updateItemRequest)
                .thenApply(response -> {
                    return response;
                })
                .exceptionally(e -> {
                    LOG.error("Failed to update item in DynamoDB with key: " + key + " - " + e.getMessage());
                    return null;
                });
    }



}
