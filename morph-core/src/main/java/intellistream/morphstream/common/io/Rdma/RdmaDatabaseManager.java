package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;
import intellistream.morphstream.common.io.Rdma.Channel.RdmaNode;
import intellistream.morphstream.common.io.Rdma.Listener.RdmaConnectionListener;
import intellistream.morphstream.common.io.Rdma.Memory.Manager.DatabaseBufferManager;
import intellistream.morphstream.common.io.Rdma.Msg.DBWRegionTokenGroup;
import intellistream.morphstream.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class RdmaDatabaseManager {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaDatabaseManager.class);
    private boolean isDriver;
    private final RdmaNode rdmaNode;
    private final String databaseHost;
    private final int databasePort;
    private final String[] workerHosts;
    private final DatabaseBufferManager databaseBufferManager;
    private final String[] tableNames;
    private final int[] valueSize;
    private final int[] numItems;
    public RdmaDatabaseManager(boolean isDriver, Configuration conf) throws Exception {
        this.isDriver = isDriver;
        this.databaseHost = conf.getString("morphstream.rdma.databaseHost");
        this.databasePort = conf.getInt("morphstream.rdma.databasePort");
        workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.rdma.workerHosts").split(",");
        rdmaNode = new RdmaNode(databaseHost ,databasePort , conf.rdmaChannelConf, RdmaChannel.RdmaChannelType.RDMA_WRITE_REQUESTOR, this.isDriver, true);
        databaseBufferManager = (DatabaseBufferManager) rdmaNode.getRdmaBufferManager();
        tableNames = conf.getString("tableNames", "table1,table2").split(";");
        valueSize = new int[tableNames.length];
        numItems = new int[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            valueSize[i] = MorphStreamEnv.get().configuration().getInt(tableNames[i] + "_value_size");
            numItems[i] = MorphStreamEnv.get().configuration().getInt(tableNames[i] + "_num_items");
        }
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
}
