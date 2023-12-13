package intellistream.morphstream.common.io.Rdma.Conf;

import com.ibm.disni.verbs.IbvContext;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;

import java.io.IOException;
import java.io.Serializable;

public class RdmaChannelConf implements Serializable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaChannelConf.class);
    private RdmaChannel.RdmaChannelType rdmaChannelType = RdmaChannel.RdmaChannelType.RDMA_READ_REQUESTOR;
    private boolean swFlowControl= false;

    //Is it strictly guaranteed that Work Request Element is consumed sequentially?
    private boolean swOrderControl= true;

    private int recvQueueDepth= 4096;

    private int sendQueueDepth= 4096;

    private int rdmaCmEventTimeout= 20000;

    private int teardownListenTimeout= 50;

    private int resolvePathTimeout= 2000;

    private long maxBufferAllocationSize= 10L * 1024L * 1024L * 1024L;

    private long maxAggPrealloc= 0L;

    private long maxAggBlock= 2L * 1024L * 1024L;

    private int maxConnectionAttempts= 5;

    private int portMaxRetries= 16;

    private String cpuList ="";

    public boolean swFlowControl() {
        return swFlowControl;
    }

    public int recvQueueDepth() {
        return recvQueueDepth;
    }

    public int sendQueueDepth() {
        return sendQueueDepth;
    }

    public int rdmaCmEventTimeout() {
        return rdmaCmEventTimeout;
    }

    public int teardownListenTimeout() {
        return teardownListenTimeout;
    }

    public int resolvePathTimeout() {
        return resolvePathTimeout;
    }

    public boolean swOrderControl() {
        return swOrderControl;
    }

    public long maxBufferAllocationSize() {
        return maxBufferAllocationSize;
    }

    public RdmaChannel.RdmaChannelType getRdmaChannelType() {
        return rdmaChannelType;
    }

    public int portMaxRetries() {
        return portMaxRetries;
    }
    public String cpuList() {
        return cpuList;
    }

    public int maxConnectAttempts() {
        return maxConnectionAttempts;
    }

    public void setCpuList(String cpuList) {
        this.cpuList = cpuList;
    }
    public void setSwFlowControl(boolean swFlowControl) {
        this.swFlowControl = swFlowControl;
    }
    public void setSwOrderControl(boolean swOrderControl) {
        this.swOrderControl = swOrderControl;
    }
    public void setRecvQueueDepth(int recvQueueDepth) {
        this.recvQueueDepth = recvQueueDepth;
    }
    public void setSendQueueDepth(int sendQueueDepth) {
        this.sendQueueDepth = sendQueueDepth;
    }
    public void setRdmaCmEventTimeout(int rdmaCmEventTimeout) {
        this.rdmaCmEventTimeout = rdmaCmEventTimeout;
    }
    public void setTeardownListenTimeout(int teardownListenTimeout) {
        this.teardownListenTimeout = teardownListenTimeout;
    }
    public void setResolvePathTimeout(int resolvePathTimeout) {
        this.resolvePathTimeout = resolvePathTimeout;
    }
    public void setMaxBufferAllocationSize(long maxBufferAllocationSize) {
        this.maxBufferAllocationSize = maxBufferAllocationSize;
    }
    public void setMaxAggPrealloc(long maxAggPrealloc) {
        this.maxAggPrealloc = maxAggPrealloc;
    }
    public void setMaxAggBlock(long maxAggBlock) {
        this.maxAggBlock = maxAggBlock;
    }
    public void setMaxConnectionAttempts(int maxConnectionAttempts) {
        this.maxConnectionAttempts = maxConnectionAttempts;
    }
    public void setPortMaxRetries(int portMaxRetries) {
        this.portMaxRetries = portMaxRetries;
    }

    public void setRdmaChannelType(RdmaChannel.RdmaChannelType rdmaChannelType) {
        this.rdmaChannelType = rdmaChannelType;
    }

    public boolean useOdp(IbvContext context) {
        int rcOdpCaps = 0;
        try {
            rcOdpCaps = context.queryOdpSupport();
        } catch (IOException e) {
            LOG.warn("Failed to query ODP support: " + e.getMessage());
        }
        boolean ret = (rcOdpCaps != -1) &&
                ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_WRITE) != 0) &&
                ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_READ) != 0);
        if (!ret) {
            LOG.warn("\"ODP (On Demand Paging) is not supported for this device. \" +\n" +
                    "Please refer to the SparkRDMA wiki for more information: \" +\n" +
                    "https://github.com/Mellanox/SparkRDMA/wiki/Configuration-Properties\")");
        } else
            LOG.info("Using ODP (On Demand Paging) memory prefetch");

        return ret;
    }
}
