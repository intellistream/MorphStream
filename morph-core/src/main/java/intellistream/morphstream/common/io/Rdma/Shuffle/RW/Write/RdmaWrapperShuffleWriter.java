package intellistream.morphstream.common.io.Rdma.Shuffle.RW.Write;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleBlockResolver;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.RdmaBaseShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.ShuffleHandle;
import intellistream.morphstream.common.io.Rdma.Shuffle.RW.ShuffleWriter;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class RdmaWrapperShuffleWriter<K,V,C> implements ShuffleWriter<K,V,C> {
    private final Logger LOG = LoggerFactory.getLogger(RdmaWrapperShuffleWriter.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final boolean stopping = false;
    private ShuffleWriter writer;
    private final RdmaShuffleBlockResolver rdmaShuffleBlockResolver;
    public RdmaWrapperShuffleWriter(RdmaShuffleBlockResolver rdmaShuffleBlockResolver, RdmaBaseShuffleHandle<K,V,C> handle, int mapId) {
        this.rdmaShuffleBlockResolver = rdmaShuffleBlockResolver;
    }
    @Override
    public void write(Iterator<Pair<K, V>> records) {
        writer.write(records);
    }
}
