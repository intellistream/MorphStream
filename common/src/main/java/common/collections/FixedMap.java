package common.collections;

import java.util.LinkedHashMap;
import java.util.Map;

public class FixedMap<K, V> extends LinkedHashMap<K, V> {
    private static final long serialVersionUID = -6542891947990885228L;
    private final int max_capacity;

    public FixedMap(int initial_capacity, int max_capacity) {
        super(initial_capacity, 0.75f, false);
        this.max_capacity = max_capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > this.max_capacity;
    }
}