package engine.txn.storage.store.disk;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A least-recently used cache used for buffer management. Extends the LinkedHashMap class for
 * simplicity of implementation. The key type is the virtual page number and the value_list type is
 * Page.
 * <p>
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS PACKAGE.
 */
public class LRUCache<k extends Long, v extends Page> extends LinkedHashMap<k, v> {
    private static final long serialVersionUID = 5742689647490393408L;
    private final int cacheSize;

    public LRUCache(int cacheSize) {
        super(16, 0.75f, true);
        this.cacheSize = cacheSize;
    }

    protected boolean removeEldestEntry(Map.Entry<k, v> eldest) {
        if (size() > cacheSize) {
            eldest.getValue().flush();
            return true;
        }
        return false;
    }
}
