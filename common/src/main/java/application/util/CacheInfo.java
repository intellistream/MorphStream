package application.util;

import java.io.Serializable;
import java.util.HashMap;

public class CacheInfo implements Serializable {
    private static final long serialVersionUID = 8130687873482833491L;

    //1st string: executionNode.getOP() + srcNode.getOP()
    //2nd string: string information in files.
    private HashMap<String, String> cachedInformation = new HashMap<>();

    public synchronized void updateInfo(String key, String value) {
        cachedInformation.put(key, value);
    }

    public synchronized boolean isEmpty(String key) {
        return cachedInformation.get(key) == null;
    }

    public String Info(String key) {
        return cachedInformation.get(key);
    }
}
