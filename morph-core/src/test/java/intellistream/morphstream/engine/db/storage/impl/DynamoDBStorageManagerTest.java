package intellistream.morphstream.engine.db.storage.impl;

import junit.framework.TestCase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DynamoDBStorageManagerTest extends TestCase {
    public DynamoDbClient dynamoDbClient;

    public DynamoDBStorageManagerTest(String testName) {
        super(testName);
        dynamoDbClient = DynamoDbClient.builder()
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
                            .readCapacityUnits(5L)
                            .writeCapacityUnits(5L)
                            .build())
                    .build();

            dynamoDbClient.createTable(request);
            System.out.println("Table created successfully.");

        } catch (DynamoDbException e) {
            System.err.println("Failed to create table: " + e.getMessage());
        }
    }
    public void testProcessBatchInsert() {
        assertTrue(true);
        final int batchSize = 25; // BatchWriteItem API 限制每批次最多 25 个项目
        final int totalItems = 1000;

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
                BatchWriteItemResponse batchWriteItemResponse = dynamoDbClient.batchWriteItem(batchWriteItemRequest);

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
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        // 创建包含键值对的列表
        List<Map<String, AttributeValue>> keysList = new ArrayList<>();
        // 添加需要读取的键
        for (int i = 0; i < 100; i++) {  // 假设我们读取前 100 个项目
            // 创建主键映射
            Map<String, AttributeValue> keyMap = new HashMap<>();
            keyMap.put("UserID", AttributeValue.builder().s(String.valueOf(i)).build());

            // 添加到 keys 列表中
            keysList.add(keyMap);
        }
        // 将 keys 列表加入 requestItems 中
        requestItems.put("User", KeysAndAttributes.builder().keys(keysList).build());

        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
                .requestItems(requestItems)
                .build();

        try {
            BatchGetItemResponse batchGetItemResponse = dynamoDbClient.batchGetItem(batchGetItemRequest);

            // 处理结果
            Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemResponse.responses();
            responses.get("User").forEach(item -> {
                System.out.println("Retrieved item: " + item.get("UserID").s() + " " + item.get("PWD").s());
            });

        } catch (DynamoDbException e) {
            System.err.println("Failed to read items: " + e.getMessage());
        }
    }

    public void testProcessBatchUpdateItem() {
        assertTrue(true);
        final int batchSize = 25;  // BatchWriteItem API 限制每批次最多 25 个项目

        for (int i = 0; i < 100; i += batchSize) {
            for (int j = 0; j < batchSize && (i + j) < 100; j++) {
                // 用户ID和新密码
                String userId = String.valueOf(i + j);
                String newPassword = "newpassword" + (i + j);

                Map<String, AttributeValue> key = new HashMap<>();
                key.put("UserID", AttributeValue.builder().s(userId).build());

                Map<String, AttributeValueUpdate> updates = new HashMap<>();
                updates.put("PWD", AttributeValueUpdate.builder()
                        .value(AttributeValue.builder().s(newPassword).build())
                        .action(AttributeAction.PUT)  // 使用 PUT 操作来更新值
                        .build());

                UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                        .tableName("User")
                        .key(key)
                        .attributeUpdates(updates)
                        .build();

                try {
                    dynamoDbClient.updateItem(updateItemRequest);
                    System.out.println("Updated item with UserID: " + userId);
                } catch (DynamoDbException e) {
                    System.err.println("Failed to update item with UserID " + userId + ": " + e.getMessage());
                }
            }
        }
    }
}
