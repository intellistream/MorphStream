package intellistream.morphstream.common.io.Rdma.Shuffle.RW;

import javafx.util.Pair;

import java.util.Iterator;

public interface ShuffleWriter<K,V,C> {
    void write(Iterator<Pair<K,V>> records);
    void stop(boolean success);
}
