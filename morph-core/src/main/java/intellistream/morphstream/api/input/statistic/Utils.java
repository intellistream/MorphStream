package intellistream.morphstream.api.input.statistic;

import java.util.*;

public class Utils {
    public static HashMap<Integer, Double> assignLowScores(HashMap<Integer, Integer> map) {
        // 将 Map 的键值对转换为 List，以便排序
        List<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(map.entrySet());

        // 使用比较器根据值进行排序
        entryList.sort(Comparator.comparing(Map.Entry::getValue));

        // 创建用于存储键和分值的 Map
        HashMap<Integer, Double> sortedKeysAndValues = new LinkedHashMap<>();
        if (hasEqualMinMax(entryList)) {
            // 处理相同的最大值和最小值的情况，例如直接计算相同分值
            double sameValueScore = 1.0; // 可以根据实际需求进行调整
            for (Map.Entry<Integer, Integer> entry : entryList) {
                sortedKeysAndValues.put(entry.getKey(), sameValueScore);
            }
            return sortedKeysAndValues;
        }

        for (int i = 0; i < entryList.size(); i++) {
            double mappedScore = 1.0 - (double) i / (entryList.size() - 1); // 线性映射，确保越小的值获得更高分数
            sortedKeysAndValues.put(entryList.get(i).getKey(), mappedScore);
        }

        return sortedKeysAndValues;
    }
    public static HashMap<Integer, Double> assignHighScores(HashMap<Integer, Integer> map) {
        // 将 Map 的键值对转换为 List，以便排序
        List<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(map.entrySet());

        // 使用比较器根据值进行排序
        entryList.sort(Comparator.comparing(Map.Entry::getValue));

        // 创建用于存储键和分值的 Map
        HashMap<Integer, Double> sortedKeysAndValues = new LinkedHashMap<>();

        if (hasEqualMinMax(entryList)) {
            // 处理相同的最大值和最小值的情况，例如直接计算相同分值
            double sameValueScore = 1.0; // 可以根据实际需求进行调整
            for (Map.Entry<Integer, Integer> entry : entryList) {
                sortedKeysAndValues.put(entry.getKey(), sameValueScore);
            }
            return sortedKeysAndValues;
        }
        // 计算线性映射的斜率和截距
        double minValue = entryList.get(0).getValue();
        double maxValue = entryList.get(entryList.size() - 1).getValue();
        double slope = 1.0 / (maxValue - minValue);
        double intercept = 1 - minValue * slope;

        // 计算分值并存储键和分值
        for (Map.Entry<Integer, Integer> entry : entryList) {
            double mappedScore = slope * entry.getValue() + intercept;
            sortedKeysAndValues.put(entry.getKey(), mappedScore);
        }

        return sortedKeysAndValues;
    }
    // 检查是否存在相同的最大值和最小值
    private static boolean hasEqualMinMax(List<Map.Entry<Integer, Integer>> entryList) {
        double minValue = entryList.get(0).getValue();
        double maxValue = entryList.get(entryList.size() - 1).getValue();
        return Double.compare(minValue, maxValue) == 0;
    }
    public static int findHighestScoreKey(
            HashMap<Integer, Double> totalEventsToScoreMap,
            HashMap<Integer, Double> totalKeysToScoreMap,
            double weightTotalEvents,
            double weightTotalKeys) {

        // 初始化最高分值和对应的键
        double highestScore = Double.MIN_VALUE;
        int highestScoreKey = -1;

        // 遍历两个 HashMap 中的键
        for (Integer key : totalEventsToScoreMap.keySet()) {
            // 获取每个键对应的分数
            double scoreTotalEvents = totalEventsToScoreMap.getOrDefault(key, 0.0);
            double scoreTotalKeys = totalKeysToScoreMap.getOrDefault(key, 0.0);

            // 计算加权总分值
            double weightedScore = (weightTotalEvents * scoreTotalEvents) + (weightTotalKeys * scoreTotalKeys);

            // 更新最高分值和对应的键
            if (weightedScore > highestScore) {
                highestScore = weightedScore;
                highestScoreKey = key;
            }
        }

        return highestScoreKey;
    }

    public static void main(String[] args) {
        // 示例数据
        HashMap<Integer, Integer> totalEventsToWorkerIdMap = new HashMap<>();
        totalEventsToWorkerIdMap.put(3, 1);//1.0  //1.0
        totalEventsToWorkerIdMap.put(1, 1);//1.4 //0.5
        totalEventsToWorkerIdMap.put(2, 0);//2.0 //0.0


        // 调用排序方法
        Map<Integer, Double> sortedKeysAndValues = assignHighScores(totalEventsToWorkerIdMap);

        // 打印排序后的键和分值
        for (Map.Entry<Integer, Double> entry : sortedKeysAndValues.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Score: " + entry.getValue());
        }
    }


}
