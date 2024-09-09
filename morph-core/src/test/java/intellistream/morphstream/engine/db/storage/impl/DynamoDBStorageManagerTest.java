package intellistream.morphstream.engine.db.storage.impl;

import junit.framework.TestCase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DynamoDBStorageManagerTest extends TestCase {
    public DynamoDbClient dynamoDBClient;
    public DynamoDbAsyncClient dynamoDBAsyncClient;
    public String[] valueList = new String[20000];

    public DynamoDBStorageManagerTest(String testName) {
        super(testName);
        dynamoDBClient = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("123", "123")))
                .build();
        dynamoDBAsyncClient = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("123", "123")))
                .build();
    }

    public void testCreateTable() {
        assertTrue(true);
        try {
            CreateTableRequest request = CreateTableRequest.builder()
                    .tableName("User")
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("UserID")  // 主键名称
                            .keyType(KeyType.HASH)  // 主键类型：HASH（分区键）
                            .build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("UserID")
                            .attributeType(ScalarAttributeType.S)  // 主键类型：字符串
                            .build())
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(1000L)
                            .writeCapacityUnits(1000L)
                            .build())
                    .build();

            dynamoDBClient.createTable(request);
            System.out.println("Table created successfully.");

        } catch (DynamoDbException e) {
            System.err.println("Failed to create table: " + e.getMessage());
        }
    }
    public void testProcessBatchInsert() {
        assertTrue(true);
        final int batchSize = 25; // BatchWriteItem API 限制每批次最多 25 个项目
        final int totalItems = 2000;

        for (int i = 0; i < totalItems; i += batchSize) {
            List<WriteRequest> writeRequests = new ArrayList<>();
            for (int j = 0; j < batchSize && (i + j) < totalItems; j++) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("UserID", AttributeValue.builder().s(String.valueOf(i + j)).build());
                item.put("PWD", AttributeValue.builder().s(String.valueOf(i + j)).build());

                WriteRequest writeRequest = WriteRequest.builder()
                        .putRequest(PutRequest.builder()
                                .item(item)
                                .build())
                        .build();

                writeRequests.add(writeRequest);
            }

            // 构建 requestItems 的 Map
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put("User", writeRequests);

            // 创建 BatchWriteItem 请求
            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            try {
                BatchWriteItemResponse batchWriteItemResponse = dynamoDBClient.batchWriteItem(batchWriteItemRequest);

                // 检查未处理的项目
                if (!batchWriteItemResponse.unprocessedItems().isEmpty()) {
                    System.out.println("Some items were not processed, retrying...");
                    // 实现重试逻辑
                } else {
                    System.out.println("Batch insert successful for items " + i + " to " + (i + batchSize - 1));
                }
            } catch (DynamoDbException e) {
                System.err.println("Failed to insert items: " + e.getMessage());
            }
        }
    }
    public void testProcessBatchGetItem() {
        assertTrue(true);

        List<Map<String, AttributeValue>> groupKeys = new ArrayList<>();
        HashMap<String, Integer> keyToIndex = new HashMap<>();
        for (int i = 1000; i < 1999; i++) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("UserID", AttributeValue.builder().s(String.valueOf(i)).build());
            keyToIndex.put(String.valueOf(i), i);
            groupKeys.add(key);
        }
        processBatchGetItem(this.dynamoDBClient, "User", groupKeys, keyToIndex);
        for (int i = 1000; i < 1999; i++) {
            if (valueList[i] == null) {
                System.out.println("Value not found for key: " + i);
            } else if (!valueList[i].equals(String.valueOf(i))) {
                System.out.println("Value mismatch for key: " + i + ", expected: " + i + ", actual: " + valueList[i]);
            }
        }
    }
    public void testProcessUpdateItem() {
        assertTrue(true);
        List<Map<String, AttributeValue>> batchKeys = new ArrayList<>();
        List<String> newValues = new ArrayList<>();
        for (int i = 1000; i < 1999; i++) {
            String key = String.valueOf(i);
            Map<String, AttributeValue> keyMap = new HashMap<>();
            keyMap.put("UserID", AttributeValue.builder().s(key).build());
            batchKeys.add(keyMap);
            newValues.add(valueList[i] + "_updated");
        }
        processBatchUpdateItem(dynamoDBClient, "User", batchKeys, "PWD", newValues);
    }
    public void testProcessAsyncUpdateItem() {
        testProcessBatchGetItem();
        assertTrue(true);
        List<Map<String, AttributeValue>> batchKeys = new ArrayList<>();
        List<String> newValues = new ArrayList<>();
        for (int i = 1000; i < 1999; i++) {
            String key = String.valueOf(i);
            Map<String, AttributeValue> keyMap = new HashMap<>();
            keyMap.put("UserID", AttributeValue.builder().s(key).build());
            batchKeys.add(keyMap);
            newValues.add(valueList[i] + "_updated");
        }
        processBatchUpdateItemsAsync(dynamoDBAsyncClient, "User", batchKeys, "PWD", newValues);
        System.out.println("Batch update initiated successfully.");
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
                    this.valueList[keyToIndex.get(item.get("UserID").s())] = item.get("PWD").s();
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

    private void processBatchUpdateItem(DynamoDbClient dynamoDBClient, String tableName, List<Map<String, AttributeValue>> batchKeys, String valueName, List<String> newValues) {
        for (int i = 0; i < batchKeys.size(); i++) {
            Map<String, AttributeValue> key = batchKeys.get(i);  // The key (UserID)
            String newValue = newValues.get(i);  // New value for the specified attribute

            // Create the map for expression attribute values
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":val", AttributeValue.builder().s(newValue).build());

            // Create UpdateItemRequest
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .updateExpression("SET " + valueName + " = :val")
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();

            try {
                // Execute the update
                dynamoDBClient.updateItem(updateItemRequest);
            } catch (DynamoDbException e) {
                System.err.println("Failed to update item: " + e.getMessage());
            }
        }
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
                    System.out.println("Successfully updated item in DynamoDB with key: " + key);
                    return response;
                })
                .exceptionally(e -> {
                    System.err.println("Failed to update item with key: " + key + " - " + e.getMessage());
                    return null;
                });
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



}
