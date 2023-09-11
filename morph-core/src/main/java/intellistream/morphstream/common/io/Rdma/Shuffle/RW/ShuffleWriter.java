package intellistream.morphstream.common.io.Rdma.Shuffle.RW;

import javafx.util.Pair;

import java.util.Iterator;

public interface ShuffleWriter<K,V,C> {
    public void write(Iterator<Pair<K,V>> records);
}
