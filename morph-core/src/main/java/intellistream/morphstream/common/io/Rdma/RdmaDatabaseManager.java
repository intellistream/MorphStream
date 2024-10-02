package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.Manager.DatabaseBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.DBWRegionTokenGroup;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.util.FixedLengthRandomString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaDatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaDatabaseManager.class);
    private boolean isDriver;
    private RdmaNode rdmaNode;
    private String databaseHost;
    private int databasePort;
    private String[] workerHosts;
    private DatabaseBufferManager databaseBufferManager;
    public ConcurrentHashMap<String, String> tableNameToValueName = new ConcurrentHashMap<>();
    private final String[] tableNames;
    private final int[] valueSize;
    private final int[] numItems;
    public DynamoDbClient dynamoDBClient;
    private long r_w_capacity_unit = 10L;
    public RdmaDatabaseManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        tableNames = conf.getString("tableNames", "table1,table2").split(";");
        valueSize = new int[tableNames.length];
        numItems = new int[tableNames.length];
        r_w_capacity_unit = conf.getLong("r_w_capacity_unit", 10L);

        for (int i = 0; i < tableNames.length; i++) {
            valueSize[i] = MorphStreamEnv.get().configuration().getInt(tableNames[i] + "_value_size");
            numItems[i] = MorphStreamEnv.get().configuration().getInt(tableNames[i] + "_num_items");
            tableNameToValueName.put(tableNames[i], tableNames[i] + "_value_names");
        }
        if (conf.getBoolean("isDynamoDB")) {
            initDynamoDatabase();
        } else if (conf.getBoolean("isTiKV")) {
            initTiKVDatabase();
        } else {
            initRDMADatabase(conf);
        }
    }
    public void initDynamoDatabase() {
        dynamoDBClient = DynamoDbClient.builder().region(Region.US_EAST_2).build();
        for (int i = 0; i < tableNames.length; i++) {
            createAndInitTableForDynamoDB(tableNames[i], numItems[i], tableNameToValueName.get(tableNames[i]));
        }
    }
    public void initTiKVDatabase() {
        String[] pdHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.databaseHost").split(",");
        int port = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.databasePort");
        StringBuilder pdAddress = new StringBuilder();
        for (String pdHost : pdHosts) {
            pdAddress.append(pdHost).append(":").append(port).append(",");
        }
        pdAddress.deleteCharAt(pdAddress.length() - 1);
        TiConfiguration tiConfiguration = TiConfiguration.createRawDefault(pdAddress.toString());
        TiSession tiSession = TiSession.create(tiConfiguration);
        RawKVClient rawKVClient = tiSession.createRawClient();
        for (int i = 0; i < tableNames.length; i++) {
            for (int j = 0; j < numItems[i]; j++) {
                rawKVClient.put(ByteString.copyFromUtf8(tableNames[i] + "_" + j), ByteString.copyFrom(FixedLengthRandomString.generateRandomFixedLengthString(valueSize[i]).getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    public void initRDMADatabase (Configuration conf) throws Exception{
        this.databaseHost = conf.getString("morphstream.rdma.databaseHost");
        this.databasePort = conf.getInt("morphstream.rdma.databasePort");
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        rdmaNode = new RdmaNode(databaseHost ,databasePort , conf.rdmaChannelConf, RdmaChannel.RdmaChannelType.RDMA_WRITE_RESPONDER, this.isDriver, true);
        databaseBufferManager = (DatabaseBufferManager) rdmaNode.getRdmaBufferManager();
        databaseBufferManager.perAllocateDatabaseBuffer(this.tableNames, this.valueSize, this.numItems);
        rdmaNode.bindConnectCompleteListener(new RdmaConnectionListener() {

            @Override
            public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) throws Exception {
                LOG.info("Database accepts " + inetSocketAddress.toString());
                for (String workerHost : workerHosts) {
                    if (workerHost.equals(inetSocketAddress.getHostName())) {
                        //Send region token to worker
                        DBWRegionTokenGroup dbwRegionTokenGroup = new DBWRegionTokenGroup();
                        for (String tableName : tableNames) {
                            dbwRegionTokenGroup.addRegionToken(databaseBufferManager.getTableNameToDatabaseBuffer().get(tableName).createRegionToken());
                        }
                        rdmaNode.sendRegionTokenToRemote(rdmaChannel, dbwRegionTokenGroup.getRegionTokens(), inetSocketAddress.getHostName());
                        //Receive region token from worker
                    }
                }
            }

            @Override
            public void onFailure(Throwable exception) {
                LOG.warn("Database connection failed", exception);
            }
        });
    }
    private void createAndInitTableForDynamoDB(String tableName, int numberItems, String valueName) {
        int batchSize = 25;
        try {
            CreateTableRequest request = CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("id")  // 主键名称
                            .keyType(KeyType.HASH)  // 主键类型：HASH（分区键）
                            .build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("id")
                            .attributeType(ScalarAttributeType.S)  // 主键类型：字符串
                            .build())
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits(r_w_capacity_unit)
                            .writeCapacityUnits(r_w_capacity_unit)
                            .build())
                    .build();

            dynamoDBClient.createTable(request);
            LOG.info("Table created successfully.");
        } catch (DynamoDbException e) {
            LOG.error("Failed to create table: " + e.getMessage());
        }

        for (int i = 0; i < numberItems; i += batchSize) {
            List<WriteRequest> writeRequests = new ArrayList<>();
            for (int j = 0; j < batchSize && (i + j) < numberItems; j++) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("id", AttributeValue.builder().s(String.valueOf(i + j)).build());
                item.put(valueName, AttributeValue.builder().s(String.valueOf(i + j)).build());

                WriteRequest writeRequest = WriteRequest.builder()
                        .putRequest(PutRequest.builder()
                                .item(item)
                                .build())
                        .build();

                writeRequests.add(writeRequest);
            }

            // 构建 requestItems 的 Map
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(tableName, writeRequests);

            // 创建 BatchWriteItem 请求
            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            try {
                BatchWriteItemResponse batchWriteItemResponse = dynamoDBClient.batchWriteItem(batchWriteItemRequest);

                // 检查未处理的项目
                if (!batchWriteItemResponse.unprocessedItems().isEmpty()) {
                    LOG.info("Retrying unprocessed items for table: " + tableName);
                    // 实现重试逻辑
                } else {
                    LOG.info("Batch write succeeded for table: " + tableName);
                }
            } catch (DynamoDbException e) {
                LOG.error("Failed to write items to table: " + tableName + " - " + e.getMessage());
            }
        }
    }
}
