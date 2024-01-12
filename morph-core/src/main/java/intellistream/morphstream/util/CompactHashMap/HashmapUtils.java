package intellistream.morphstream.util.CompactHashMap;

import java.util.*;

public class HashmapUtils {
    public static List<Integer> sortValuesByKeys(HashMap<Integer, Integer> hashMap) {
        // 将HashMap的键放入List
        List<Integer> keys = new ArrayList<>(hashMap.keySet());

        // 对List进行排序
        Collections.sort(keys);

        // 创建用于存储排序后值的List
        List<Integer> sortedValues = new ArrayList<>();

        // 遍历排序后的List，将对应的值加入新的List
        for (Integer key : keys) {
            sortedValues.add(hashMap.get(key));
        }

        return sortedValues;
    }
    public static List<Integer> sortKeysByValues(HashMap<Integer, Integer> map) {
        // 将 Map 的键值对转换为 List，以便排序
        List<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(map.entrySet());

        // 使用比较器根据值进行排序
        entryList.sort(Comparator.comparing(Map.Entry::getValue));

        // 创建用于存储排序后键的 List
        List<Integer> sortedKeys = new ArrayList<>();

        // 遍历排序后的 List，将键加入新的 List
        for (Map.Entry<Integer, Integer> entry : entryList) {
            sortedKeys.add(entry.getKey());
        }

        return sortedKeys;
    }
}
