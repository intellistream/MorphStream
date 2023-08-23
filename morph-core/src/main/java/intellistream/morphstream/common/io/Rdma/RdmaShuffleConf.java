package intellistream.morphstream.common.io.Rdma;

import com.ibm.disni.verbs.IbvContext;
import intellistream.morphstream.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.common.io.Utils.BytesUtils.byteStringAsBytes;

public class RdmaShuffleConf {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleConf.class);
    private Configuration conf;
    public RdmaShuffleConf(Configuration conf) {
        this.conf = conf;
    }
    private String getRdmaConfKey(String name) {
        return getConfKey(toRdmaConfKey(name));
    }
    private int getRdmaConfIntInRange(String name, int defaultValue, int min, int max) {
        int value = conf.getInt(toRdmaConfKey(name), defaultValue);
        if (value < min || value > max) {
            return defaultValue;
        }
        return value;
    }
    public String getConfKey(String name) {
        return conf.getString(name);
    }
    private String toRdmaConfKey(String name) {
        return "morphstream.rdma." + name;
    }
    public void setDriverPort(String value) {
        conf.put(toRdmaConfKey("driverPort"), value);
    }
    private long getRdmaConfSizeAsBytesInRange(String name, String defaultValue, String min, String max) {
        long value = byteStringAsBytes(conf.getString(toRdmaConfKey(name), defaultValue));
        long minBytes = byteStringAsBytes(min);
        long maxBytes = byteStringAsBytes(max);
        if (value < minBytes || value > maxBytes) {
            return byteStringAsBytes(defaultValue);
        }
        return value;
    }
    public final int recvQueueDepth = getRdmaConfIntInRange("recvQueueDepth", 256, 256, 65535);
    public final int sendQueueDepth = getRdmaConfIntInRange("sendQueueDepth", 4096, 256, 65535);
    public final int recvWrSize = (int) getRdmaConfSizeAsBytesInRange("recvWrSize", "4k", "2k", "1m");
    public final boolean swFlowControl = conf.getBoolean(toRdmaConfKey("swFlowControl"), true);
    public final long maxBufferAllocationSize = getRdmaConfSizeAsBytesInRange(
            "maxBufferAllocationSize", "10g", "0", "10t");
    public final int rdmaDeviceNum = conf.getInt(toRdmaConfKey("device.num"), 0);
    public boolean useOdp(IbvContext context) throws IOException {
        boolean odpEnabled = conf.getBoolean(toRdmaConfKey("useOdp"), false);
        if (odpEnabled) {
            int rcOdpCaps = context.queryOdpSupport();
            boolean ret = (rcOdpCaps != -1) &&
                    ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_WRITE) != 0) &&
                    ((rcOdpCaps & IbvContext.IBV_ODP_SUPPORT_READ) != 0);
            if (!ret) {
                LOG.warn("ODP (On Demand Paging) is not supported for this device. " +
                        "Please refer to the SparkRDMA wiki for more information: " +
                        "https://github.com/Mellanox/SparkRDMA/wiki/Configuration-Properties");
            } else {
                LOG.info("Using ODP (On Demand Paging) memory prefetch");
            }
            return ret;
        }
        return false;
    }
    public final String cpuList = getRdmaConfKey("cpuList");

    public final long shuffleWriteBlockSize = getRdmaConfSizeAsBytesInRange(
            "shuffleWriteBlockSize", "8m", "4k", "512m");

    public final long shuffleReadBlockSize = getRdmaConfSizeAsBytesInRange(
            "shuffleReadBlockSize", "256k", "0", "512m");

    public final long maxBytesInFlight = getRdmaConfSizeAsBytesInRange(
            "maxBytesInFlight", "48m", "128k", "100g");
    public final Map<Integer, Integer> preAllocateBuffers;

    {
        String[] bufferEntries = getRdmaConfKey("preAllocateBuffers").split(",");
        Map<Integer, Integer> bufferMap = new HashMap<>();
        long totalBytes = 0;
        for (String entry : bufferEntries) {
            if (!entry.isEmpty()) {
                String[] parts = entry.split(":");
                if (parts.length == 2) {
                    int bufferSize = (int) byteStringAsBytes(parts[0].trim());
                    int bufferCount = Integer.parseInt(parts[1].trim());
                    bufferMap.put(bufferSize, bufferCount);
                    totalBytes += bufferSize * bufferCount;
                }
            }
        }
        if (totalBytes >= maxBufferAllocationSize) {
            try {
                throw new Exception("Total pre allocation buffer size >= " + "morphstream.rdma.maxBufferAllocationSize");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        preAllocateBuffers = bufferMap;
    }
    public final boolean collectShuffleReaderStats = conf.getBoolean(toRdmaConfKey("collectShuffleReaderStats"),false);

    public final int partitionLocationFetchTimeout = getRdmaConfIntInRange(
            "partitionLocationFetchTimeout", 120000, 1000, Integer.MAX_VALUE);

    public final int fetchTimeBucketSizeInMs = getRdmaConfIntInRange(
            "fetchTimeBucketSizeInMs", 300, 5, 60000);

    public final int fetchTimeNumBuckets = getRdmaConfIntInRange("fetchTimeNumBuckets", 5, 2, 100);

    public final boolean collectOdpStats = conf.getBoolean(toRdmaConfKey("collectOdpStats"), true);

    public final String driverHost = conf.getString("spark.driver.host");

    public final int driverPort = getRdmaConfIntInRange("driverPort", 0, 1025, 65535);

    public final int executorPort = getRdmaConfIntInRange("executorPort", 0, 1025, 65535);

    public final int portMaxRetries = conf.getInt("spark.port.maxRetries", 16);

    public final int rdmaCmEventTimeout = getRdmaConfIntInRange("rdmaCmEventTimeout", 20000, -1, 60000);

    public final int teardownListenTimeout = getRdmaConfIntInRange("teardownListenTimeout", 50, -1, 60000);

    public final int resolvePathTimeout = getRdmaConfIntInRange("resolvePathTimeout", 2000, -1, 60000);

    public final int maxConnectionAttempts = getRdmaConfIntInRange("maxConnectionAttempts", 5, 1, 100);
}
