package intellistream.morphstream.common.io.Rdma.Memory.Manager;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CacheBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.CircularMessageBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.OwnershipTableBuffer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class WorkerRdmaBufferManager extends RdmaBufferManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerRdmaBufferManager.class);
    private CircularMessageBuffer circularMessageBuffer;//Receive events for worker
    private final ConcurrentHashMap<Integer, CircularMessageBuffer> remoteOperationsMap = new ConcurrentHashMap<>();//workerId -> CircularRdmaBuffer
    private OwnershipTableBuffer tableBuffer;//Receive ownership table for worker
    private CacheBuffer cacheBuffer;//Shared cache buffer among workers
    public WorkerRdmaBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        super(pd, conf);
    }
    public void perAllocateCircularRdmaBuffer(int length, int totalThreads) throws Exception {
        if (circularMessageBuffer == null) {
            circularMessageBuffer = new CircularMessageBuffer(getPd(), length, totalThreads);
        }
        LOG.info("Pre allocated request buffer of size {} MB for driver", (length / 1024/1024));
    }
    public void perAllocateTableBuffer(int length, int totalThreads) throws Exception {
        if (tableBuffer == null) {
            tableBuffer = new OwnershipTableBuffer(getPd(), length, totalThreads);
        }
        LOG.info("Pre allocated ownership table of size {} MB for driver", (length / 1024/1024));
    }
    public void perAllocateCacheBuffer(int workId, int length, String[] tableNames, int[] valueSize, int tthread) throws Exception {
        if (cacheBuffer == null) {
            cacheBuffer = new CacheBuffer(workId, getPd(), length, tableNames, valueSize, tthread);
        }
        LOG.info("Pre allocated global data table of size {} MB for workers", (length / 1024/1024));
    }

    public void perAllocateRemoteOperationBuffer(int totalWorkers, int length, int totalThreads) throws Exception {
        for (int i = 0; i < totalWorkers; i++) {
            if (remoteOperationsMap.get(i) == null) {
                remoteOperationsMap.put(i, new CircularMessageBuffer(getPd(), length, totalThreads));
            }
        }
        LOG.info("Pre allocated remote operation buffers of size {} MB for each worker", (length / 1024/1024));
    }
    public CircularMessageBuffer getRemoteOperationBuffer(int workerId) {
        return remoteOperationsMap.get(workerId);
    }

}
