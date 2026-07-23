package intellistream.morphstream.common.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * Minimal immutable pair used by MorphStream's internal data paths.
 */
public final class Pair<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final K key;
    private final V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Pair)) {
            return false;
        }
        Pair<?, ?> pair = (Pair<?, ?>) other;
        return Objects.equals(key, pair.key) && Objects.equals(value, pair.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
