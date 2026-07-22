package intellistream.morphstream.common.io.Rdma.Shuffle.RW;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

public interface ShuffleWriter<K,V,C> {
    void write(Iterator<Pair<K,V>> records);
    void stop(boolean success);
}
