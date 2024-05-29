package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.output.ResultBatch;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaCompletionListener;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CircularMessageBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.OwnershipTableBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Manager.WorkerRdmaBufferManager;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.Msg.*;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.RemoteOperationBatch;
import lombok.Getter;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RdmaWorkerManager implements Serializable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaWorkerManager.class);
    private final RdmaNode rdmaNode;
    private final int totalFunctionExecutors;
    private final boolean isDriver;
    @Getter
    private final int managerId;
    private final int workerNum;
    private final String[] workerHosts;
    private final String[] workerPorts;
    private final String driverHost;
    private final int driverPort;
    private final String databaseHost;
    private final int databasePort;
    private final Configuration conf;
    private final WorkerRdmaBufferManager rdmaBufferManager;
    private RdmaChannel driverRdmaChannel;
    private RdmaChannel databaseRdmaChannel;
    private DWRegionTokenGroup dwRegionTokenGroup = new DWRegionTokenGroup();// Driver's region token
    private DBWRegionTokenGroup dbwRegionTokenGroup = new DBWRegionTokenGroup();// Database's region token
    private ResultBatch resultBatch;// Results, send to driver
    private ConcurrentHashMap<Integer, RemoteOperationBatch> remoteOperationBatchMap = new ConcurrentHashMap<>(); //Remote operations, send to workers
    private ConcurrentHashMap<Integer, RdmaChannel> workerRdmaChannelMap = new ConcurrentHashMap<>();//workerId -> RdmaChannel
    private ConcurrentHashMap<Integer, WWRegionTokenGroup> workerRegionTokenMap = new ConcurrentHashMap<>();//workerId -> RegionToken

    public RdmaWorkerManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        this.conf = conf;
        this.totalFunctionExecutors = MorphStreamEnv.get().configuration().getInt("tthread");
        this.workerNum = MorphStreamEnv.get().configuration().getInt("workerNum", 1);
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerPorts").split(",");
        managerId = MorphStreamEnv.get().configuration().getInt("workerId", 0);
        driverHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.driverHost");
        driverPort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.driverPort");
        databaseHost = MorphStreamEnv.get().configuration().getString("morphstream.rdma.databaseHost");
        databasePort = MorphStreamEnv.get().configuration().getInt("morphstream.rdma.databasePort");
        rdmaNode = new RdmaNode(workerHosts[managerId],  Integer.parseInt(workerPorts[managerId]), conf.rdmaChannelConf, conf.rdmaChannelConf.getRdmaChannelType(), isDriver, false);
        rdmaBufferManager = (WorkerRdmaBufferManager) rdmaNode.getRdmaBufferManager();
        //PreAllocate message buffer
        rdmaBufferManager.perAllocateCircularRdmaBuffer(MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"));
        rdmaBufferManager.perAllocateTableBuffer(MorphStreamEnv.get().configuration().getInt("TableBufferCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"));
        String[] tableName = MorphStreamEnv.get().configuration().getString("tableNames","table1,table2").split(";");
        int[] valueSize = new int[tableName.length];
        for (int i = 0; i < tableName.length; i++) {
            valueSize[i] = MorphStreamEnv.get().configuration().getInt(tableName[i] + "_value_size");
        }
        rdmaBufferManager.perAllocateCacheBuffer(this.managerId, MorphStreamEnv.get().configuration().getInt("CacheBufferCapacity"), tableName, valueSize, MorphStreamEnv.get().configuration().getInt("tthread"));
        rdmaBufferManager.perAllocateRemoteOperationBuffer(MorphStreamEnv.get().configuration().getInt("workerNum"), MorphStreamEnv.get().configuration().getInt("CircularBufferCapacity"), MorphStreamEnv.get().configuration().getInt("tthread"));
        resultBatch = new ResultBatch(MorphStreamEnv.get().configuration().getInt("maxResultsCapacity"), MorphStreamEnv.get().configuration().getInt("frontendNum"), this.totalFunctionExecutors);
        for (int i = 0; i < MorphStreamEnv.get().configuration().getInt("workerNum"); i++) {
            if (i == managerId) continue;
            remoteOperationBatchMap.put(i, new RemoteOperationBatch(MorphStreamEnv.get().configuration().getInt("tthread"), this.totalFunctionExecutors));
        }
        //Result count to decide whether to send the batched results
        SOURCE_CONTROL.getInstance().config(MorphStreamEnv.get().configuration().getInt("frontendNum"), MorphStreamEnv.get().configuration().getInt("sendMessagePerFrontend"), this.totalFunctionExecutors, MorphStreamEnv.get().configuration().getInt("returnResultPerExecutor"));

        //Wait for other workers to connect
        rdmaNode.bindConnectCompleteListener(new RdmaConnectionListener() {
            @Override
            public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) throws Exception {
                for (int i = 0; i < workerNum; i++) {
                    if (workerHosts[i].equals(inetSocketAddress.getHostName())) {
                        workerRdmaChannelMap.put(i, rdmaChannel);
                        //Send region token to target workers
                        WWRegionTokenGroup wwRegionTokenGroup = new WWRegionTokenGroup();
                        List<RegionToken> regionTokens = rdmaBufferManager.getCacheBuffer().createRegionTokens();
                        regionTokens.add(rdmaBufferManager.getRemoteOperationBuffer(i).createRegionToken());
                        wwRegionTokenGroup.addRegionTokens(regionTokens);
                        rdmaNode.sendRegionTokenToRemote(rdmaChannel, wwRegionTokenGroup.getRegionTokens(), inetSocketAddress.getHostName());

                        //Receive region token from target workers
                        workerRegionTokenMap.put(i, new WWRegionTokenGroup());
                        workerRegionTokenMap.get(i).addRegionTokens(rdmaNode.getRemoteRegionToken(rdmaChannel));
                    }
                }
                MorphStreamEnv.get().workerLatch().countDown();
                LOG.info("Worker accepts " + inetSocketAddress.toString());
            }
            @Override
            public void onFailure(Throwable exception) {
                LOG.warn("Worker fails to accept " + exception.getMessage());
            }
        });
        MorphStreamEnv.get().workerLatch().countDown();

        //Connect to other workers
        for (int i = managerId + 1; i < workerNum; i++) {
            if (i != managerId) {
                workerRdmaChannelMap.put(i, rdmaNode.getRdmaChannel(new InetSocketAddress(workerHosts[i], Integer.parseInt(workerPorts[i])), true, conf.rdmaChannelConf.getRdmaChannelType()));
                workerRegionTokenMap.put(i, new WWRegionTokenGroup());
                //Receive region token from target workers
                workerRegionTokenMap.get(i).addRegionTokens(rdmaNode.getRemoteRegionToken(workerRdmaChannelMap.get(i)));

                //Send region token to target workers
                WWRegionTokenGroup wwRegionTokenGroup = new WWRegionTokenGroup();
                List<RegionToken> regionTokens = rdmaBufferManager.getCacheBuffer().createRegionTokens();
                regionTokens.add(rdmaBufferManager.getRemoteOperationBuffer(i).createRegionToken());
                wwRegionTokenGroup.addRegionTokens(regionTokens);
                rdmaNode.sendRegionTokenToRemote(workerRdmaChannelMap.get(i), wwRegionTokenGroup.getRegionTokens(), workerHosts[i]);
                MorphStreamEnv.get().workerLatch().countDown();
            }
        }
        //Wait for other workers to connect
        MorphStreamEnv.get().workerLatch().await();
    }
    public void connectDatabase() throws Exception {
        //Connect to database and receive region token from remoteDatabase;
        databaseRdmaChannel = rdmaNode.getRdmaChannel(new InetSocketAddress(databaseHost, databasePort), true, conf.rdmaChannelConf.getRdmaChannelType());
        dbwRegionTokenGroup.addRegionTokens(rdmaNode.getRemoteRegionToken(databaseRdmaChannel));
    }
    public void connectDriver() throws Exception {
        //Connect to driver and receive region token from driver
        driverRdmaChannel = rdmaNode.getRdmaChannel(new InetSocketAddress(driverHost, driverPort), true, conf.rdmaChannelConf.getRdmaChannelType());
        dwRegionTokenGroup.addRegionTokens(rdmaNode.getRemoteRegionToken(driverRdmaChannel));

        //Send region token to driver
        WDRegionTokenGroup wdRegionTokenGroup = new WDRegionTokenGroup();
        wdRegionTokenGroup.addRegionToken(rdmaBufferManager.getCircularMessageBuffer().createRegionToken());
        wdRegionTokenGroup.addRegionToken(rdmaBufferManager.getTableBuffer().createRegionToken());
        rdmaNode.sendRegionTokenToRemote(driverRdmaChannel, wdRegionTokenGroup.getRegionTokens(), driverHost);
    }
    public CircularMessageBuffer getCircularRdmaBuffer() {
        return rdmaBufferManager.getCircularMessageBuffer();
    }
    public OwnershipTableBuffer getTableBuffer() { return rdmaBufferManager.getTableBuffer();}
    public CacheBuffer getCacheBuffer() { return rdmaBufferManager.getCacheBuffer();}
    public CircularMessageBuffer getRemoteOperationsBuffer(int i) {
        return rdmaBufferManager.getRemoteOperationBuffer(i);
    }

    public void sendResults(int senderThreadId, FunctionMessage functionMessage) {
        this.resultBatch.add(senderThreadId, functionMessage);
        MeasureTools.WorkerFinishEndTime(senderThreadId);
    }
    public void sendResultBatch(int senderThreadId) throws Exception {
        if (this.resultBatch.getAllResultCount() == 0) return;
        MeasureTools.WorkerRdmaSendResultStartTime(senderThreadId);
        SOURCE_CONTROL.getInstance().workerStartSendResultBarrier();
        if (senderThreadId == 0){
            ByteBuffer byteBuffer = this.resultBatch.buffer();
            byteBuffer.flip();

            RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
            ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();
            dataBuffer.put(byteBuffer);
            dataBuffer.flip();

            RdmaChannel rdmaChannel = driverRdmaChannel;
            RegionToken regionToken = dwRegionTokenGroup.getResultsToken();

            long remoteAddress = regionToken.getAddress();
            int rkey = regionToken.getLocalKey();
            CountDownLatch latch = new CountDownLatch(1);
            rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buffer, Integer imm) {
                    try {
                        rdmaBuffer.getByteBuffer().clear();
                        rdmaBufferManager.put(rdmaBuffer);
                        regionToken.setAddress(remoteAddress + byteBuffer.capacity());
                        LOG.info("Worker " + managerId + " sends " + resultBatch.getAllResultCount() + " results" + " to driver.");
                        resultBatch.clear();
                        latch.countDown();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                @Override
                public void onFailure(Throwable exception) {
                    try {
                        rdmaBuffer.getByteBuffer().clear();
                        rdmaBufferManager.put(rdmaBuffer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(), rdmaBuffer.getLkey(), remoteAddress, rkey);
            latch.await();
        }
        SOURCE_CONTROL.getInstance().workerStartSendResultBarrier();
        MeasureTools.WorkerRdmaSendResultEndTime(senderThreadId);
    }
    public void sendRemoteOperations(int senderThreadId, int receiverWorkerId, FunctionMessage functionMessage) {
        this.remoteOperationBatchMap.get(receiverWorkerId).addMessage(senderThreadId, functionMessage);
    }
    public void sendRemoteOperationBatch(int senderId) throws Exception {
        if (this.remoteOperationBatchMap.get(senderId) == null) return;
        RemoteOperationBatch remoteOperationBatch = this.remoteOperationBatchMap.get(senderId);
        ByteBuffer byteBuffer = remoteOperationBatch.buffer();
        byteBuffer.flip();

        RdmaBuffer rdmaBuffer = rdmaBufferManager.get(byteBuffer.capacity());
        ByteBuffer dataBuffer = rdmaBuffer.getByteBuffer();
        dataBuffer.put(byteBuffer);
        dataBuffer.flip();

        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(senderId);
        RegionToken regionToken = workerRegionTokenMap.get(senderId).getRemoteOperationToken();

        long remoteAddress = regionToken.getAddress();
        int rkey = regionToken.getLocalKey();
        CountDownLatch latch = new CountDownLatch(1);
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                    regionToken.setAddress(remoteAddress + byteBuffer.capacity());
                    LOG.info(String.format("Worker (%d) sends (%d) remote operations to worker (%d)", managerId, remoteOperationBatch.getTotalMessagesSize(), senderId));
                    remoteOperationBatch.clear();
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public void onFailure(Throwable exception) {
                try {
                    rdmaBuffer.getByteBuffer().clear();
                    rdmaBufferManager.put(rdmaBuffer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(), rdmaBuffer.getLkey(), remoteAddress, rkey);
        latch.await();
    }
    public String syncReadRemoteCache(int workerId, int keyIndex, int tableIndex, int length) throws Exception {
        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workerId);
        RegionToken regionToken = workerRegionTokenMap.get(workerId).getRegionTokens().get(tableIndex);

        long remoteAddress = regionToken.getAddress() + keyIndex;
        int rkey = regionToken.getLocalKey();

        RdmaBuffer readData = rdmaBufferManager.get(length);
        ByteBuffer dataBuffer = readData.getByteBuffer();

        AtomicReference<String> result = new AtomicReference<>("false");

        CountDownLatch latch = new CountDownLatch(1);
        rdmaChannel.rdmaReadInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                int ownershipId = dataBuffer.getShort();
                ByteBuffer valueBuffer = dataBuffer.slice();
                String value = StandardCharsets.UTF_8.decode(valueBuffer).toString();
                if (ownershipId == managerId) {
                    result.set(value);
                }
                rdmaBufferManager.put(readData);
                latch.countDown();
            }
            @Override
            public void onFailure(Throwable exception) {
                rdmaBufferManager.put(readData);
                latch.countDown();
            }
        }, readData.getAddress(), readData.getLkey(), new int[]{length}, new long[]{remoteAddress}, new int[]{rkey});
        latch.await();
        return result.get();
    }
    public void syncWriteRemoteCache(int workerId, int keyIndex, int tableIndex, String value) throws Exception {
        RdmaChannel rdmaChannel = workerRdmaChannelMap.get(workerId);
        RegionToken regionToken = workerRegionTokenMap.get(workerId).getRegionTokens().get(tableIndex);

        long remoteAddress = regionToken.getAddress() + keyIndex;
        int rkey = regionToken.getLocalKey();

        RdmaBuffer readData = rdmaBufferManager.get(value.getBytes(StandardCharsets.UTF_8).length + 2);
        ByteBuffer dataBuffer = readData.getByteBuffer();

        dataBuffer.putShort((short) workerId);
        dataBuffer.put(value.getBytes(StandardCharsets.UTF_8));
        dataBuffer.flip();

        CountDownLatch latch = new CountDownLatch(1);
        rdmaChannel.rdmaWriteInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                rdmaBufferManager.put(readData);
                latch.countDown();
                LOG.info("Write to remote cache with workerId: " +  workerId);
            }
            @Override
            public void onFailure(Throwable exception) {
                rdmaBufferManager.put(readData);
                latch.countDown();
            }
        }, readData.getAddress(), readData.getLength(), readData.getLkey(), remoteAddress, rkey);
        latch.await();
    }
    public void close() {
        try {
            rdmaNode.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void asyncReadRemoteDatabase(int keyIndex, int tableIndex, int length, int valueIndex, String[] valueList, AtomicInteger count) throws Exception {
        RegionToken regionToken = dbwRegionTokenGroup.getRegionTokens().get(tableIndex);

        long remoteAddress = regionToken.getAddress() + keyIndex;
        int rkey = regionToken.getLocalKey();

        RdmaBuffer readData = rdmaBufferManager.get(length);
        ByteBuffer dataBuffer = readData.getByteBuffer();

        databaseRdmaChannel.rdmaReadInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer, Integer imm) {
                dataBuffer.getShort();
                ByteBuffer valueBuffer = dataBuffer.slice();
                String value = StandardCharsets.UTF_8.decode(valueBuffer).toString();
                valueList[valueIndex] = value;
                count.incrementAndGet();
                rdmaBufferManager.put(readData);
            }
            @Override
            public void onFailure(Throwable exception) {
                rdmaBufferManager.put(readData);
            }
        }, readData.getAddress(), readData.getLkey(), new int[]{length}, new long[]{remoteAddress}, new int[]{rkey});
    }
}
