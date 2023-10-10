package intellistream.morphstream.common.io.Rdma.Shuffle.RW.Write;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleBlockResolver;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.RdmaBaseShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.ShuffleDependency;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.ShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.RW.ShuffleWriter;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class RdmaWrapperShuffleWriter<K,V,C> implements ShuffleWriter<K,V,C> {
    private final Logger LOG = LoggerFactory.getLogger(RdmaWrapperShuffleWriter.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private boolean stopping = false;
    private final RdmaBaseShuffleHandle shuffleHandle;
    private ShuffleWriter writer;
    private final int mapId;
    private final RdmaShuffleBlockResolver rdmaShuffleBlockResolver;
    public RdmaWrapperShuffleWriter(RdmaShuffleBlockResolver rdmaShuffleBlockResolver, RdmaBaseShuffleHandle<K,V,C> handle, int mapId) {
        this.rdmaShuffleBlockResolver = rdmaShuffleBlockResolver;
        this.shuffleHandle = handle;
        this.mapId = mapId;
    }
    @Override
    public void write(Iterator<Pair<K, V>> records) {
        writer.write(records);
    }

    @Override
    public void stop(boolean success) {
        if (stopping) {
            return;
        }
        stopping = true;
        long startTime = System.nanoTime();
        RdmaShuffleManager shuffleManager = env.RM();
        if (success) {
            //Publish this RdmaMapTaskOutput to the Driver
            try {
                shuffleManager.publicMapTaskOutput(shuffleHandle.shuffleId, mapId, null);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
