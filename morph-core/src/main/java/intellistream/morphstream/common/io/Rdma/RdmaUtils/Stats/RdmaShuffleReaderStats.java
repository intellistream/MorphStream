package intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats;

import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaShuffleReaderStats {
    private final Logger LOG = LoggerFactory.getLogger(RdmaShuffleReaderStats.class);
    private final ConcurrentHashMap<BlockManagerId, RdmaRemoteFetchHistogram> remoteFetchHistogramMap = new ConcurrentHashMap<>();
    private RdmaRemoteFetchHistogram global;
    private RdmaShuffleConf conf;
    public RdmaShuffleReaderStats(RdmaShuffleConf conf) {
        this.conf = conf;
        this.global = new RdmaRemoteFetchHistogram(conf.fetchTimeNumBuckets, conf.fetchTimeBucketSizeInMs);
    }
    public void updateRemoteFetchHistogram(BlockManagerId blockManagerId, int fetchTimeInMs) {
        RdmaRemoteFetchHistogram remoteFetchHistogram = remoteFetchHistogramMap.get(blockManagerId);
        if (remoteFetchHistogram == null) {
            remoteFetchHistogram = new RdmaRemoteFetchHistogram(conf.fetchTimeNumBuckets, conf.fetchTimeBucketSizeInMs);
            remoteFetchHistogramMap.putIfAbsent(blockManagerId, remoteFetchHistogram);
        }
        remoteFetchHistogram.addSample(fetchTimeInMs);
        global.addSample(fetchTimeInMs);
    }
    public void printRemoteFetchHistogram() {
        for (Map.Entry<BlockManagerId, RdmaRemoteFetchHistogram> entry : remoteFetchHistogramMap.entrySet()) {
            LOG.info("Fetch blocks from " + entry.getKey().getHost() + " : " + entry.getKey().getPort() + " : " + entry.getValue().getString());
        }
        LOG.info("Total fetches from all remote hostPorts: " + global.getString());
    }

}
