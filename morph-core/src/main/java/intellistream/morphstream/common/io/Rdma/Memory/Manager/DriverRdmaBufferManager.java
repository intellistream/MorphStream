package intellistream.morphstream.common.io.Rdma.Memory.Manager;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.AllocatorStack;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.CircularMessageBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.ExecutorsServiceContext;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

@Getter
public class DriverRdmaBufferManager extends RdmaBufferManager {
    private static final Logger LOG = LoggerFactory.getLogger(DriverRdmaBufferManager.class);
    private final ConcurrentHashMap<Integer, CircularMessageBuffer> resultBufferMap = new ConcurrentHashMap<>();//workerId -> CircularRdmaBuffer
    public DriverRdmaBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        super(pd, conf);
    }
    public void perAllocateResultBuffer(int totalWorkers, int length, int totalThreads) throws Exception {
        for (int i = 0; i < totalWorkers; i++) {
            if (resultBufferMap.get(i) == null) {
                resultBufferMap.put(i, new CircularMessageBuffer(getPd(), length, totalThreads));
            }
        }
        LOG.info("Pre allocated {} buffers of size {} KB for each worker", totalWorkers, (length / 1024));
    }

    public CircularMessageBuffer getResultBuffer(int workerId) {
        return resultBufferMap.get(workerId);
    }
}
