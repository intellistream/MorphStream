package intellistream.morphstream.common.io.Rdma.Memory.Manager;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.Impl.DatabaseBuffer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

@Getter
public class DatabaseBufferManager extends RdmaBufferManager{
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseBufferManager.class);
    private final ConcurrentHashMap<String, DatabaseBuffer> tableNameToDatabaseBuffer = new ConcurrentHashMap<>();
    public DatabaseBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        super(pd, conf);
    }
    public void perAllocateDatabaseBuffer(String[] tableNames, int[] valueSize, int[] numItems) throws Exception {
       for (int i = 0; i < tableNames.length; i++) {
           if (tableNameToDatabaseBuffer.get(tableNames[i]) == null) {
               tableNameToDatabaseBuffer.put(tableNames[i], new DatabaseBuffer(tableNames[i], numItems[i], valueSize[i], getPd()));
               tableNameToDatabaseBuffer.get(tableNames[i]).initDatabaseBuffer();
           }
       }
       LOG.info("Pre allocated database buffer for each table");
    }
}
