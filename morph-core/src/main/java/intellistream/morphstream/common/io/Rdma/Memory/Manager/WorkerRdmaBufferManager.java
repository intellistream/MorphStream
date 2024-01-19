package intellistream.morphstream.common.io.Rdma.Memory.Manager;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.CircularMessageBuffer;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.TableBuffer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Getter
public class WorkerRdmaBufferManager extends RdmaBufferManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerRdmaBufferManager.class);
    private CircularMessageBuffer circularMessageBuffer;//Receive events for worker
    private TableBuffer tableBuffer;//Receive ownership table for worker
    public WorkerRdmaBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        super(pd, conf);
    }
    public void perAllocateCircularRdmaBuffer(int length, int totalThreads) throws Exception {
        if (circularMessageBuffer == null) {
            circularMessageBuffer = new CircularMessageBuffer(getPd(), length, totalThreads);
        }
    }
    public void perAllocateTableBuffer(int length, int totalThreads) throws Exception {
        if (tableBuffer == null) {
            tableBuffer = new TableBuffer(getPd(), length, totalThreads);
        }
    }
}
