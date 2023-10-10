package intellistream.morphstream.util;

import java.util.LinkedHashMap;

/**
 * Created by tony on 7/8/2017.
 */
public class myIntegerMap<key> extends LinkedHashMap<key, Integer> {
    private static final long serialVersionUID = 4202592582267245248L;

    public Integer get(Object key) {
        return super.get(key) == null ? 0 : super.get(key);
    }
}
