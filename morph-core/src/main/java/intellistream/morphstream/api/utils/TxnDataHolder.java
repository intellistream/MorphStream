package intellistream.morphstream.api.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class TxnDataHolder {
    public HashMap<String, Integer> integerMap;
    public HashMap<String, Long> longMap;
    public HashMap<String, Float> floatMap;
    public HashMap<String, Double> doubleMap;
    public HashMap<String, Boolean> booleanMap;
    public HashMap<String, String> stringMap;
    public HashMap<String, HashSet<Object>> hashSetMap;
    public HashMap<String, List<Object>> listMap;

    public TxnDataHolder() {}
}
