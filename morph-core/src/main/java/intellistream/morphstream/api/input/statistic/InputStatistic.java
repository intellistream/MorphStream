package intellistream.morphstream.api.input.statistic;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Input Statistic for each worker
 */
public class InputStatistic {
    public int workerId;
    public int totalEvents = 0;
    public ConcurrentHashMap<String, Integer> keyToNumberMap = new ConcurrentHashMap<>();
    public ConcurrentHashMap<String, keyStatistic> TableToKeyStatistic = new ConcurrentHashMap<>();
    public InputStatistic(int workerId, String[] tableName) {
        this.workerId = workerId;
        for (String table : tableName) {
            TableToKeyStatistic.put(table, new keyStatistic());
        }
    }
    public void add(List<String> keys, String tableName) {
        for (String key : keys) {
            if (TableToKeyStatistic.get(tableName).containsKey(key)) {
                TableToKeyStatistic.get(tableName).put(key, TableToKeyStatistic.get(tableName).get(key) + 1);
            } else {
                TableToKeyStatistic.get(tableName).put(key, 1);
            }
        }
        totalEvents ++;
    }
    public int getNumber(String key, String tableName) {
        return TableToKeyStatistic.get(tableName).getOrDefault(key, 0);
    }
    public void display(String[][] data, DriverSideOwnershipTable driverSideOwnershipTable) {
        int totalOperations = 0;
        int maxOperationsPerKey = 0;
        int totalKeys = keyToNumberMap.size();
        int withOwnership = 0;
        int withoutOwnership = 0;
        for (String key : keyToNumberMap.keySet()) {
            int number = keyToNumberMap.get(key);
            totalOperations += number;
            if (number > maxOperationsPerKey) {
                maxOperationsPerKey = number;
            }
            if (driverSideOwnershipTable.isWorkerOwnKey(workerId, key)) {
                withOwnership ++;
            } else {
                withoutOwnership ++;
            }
        }
        double averageOperationsPerKey = totalOperations / totalKeys;
        data[workerId][0] = String.valueOf(workerId);
        data[workerId][1] = String.valueOf(totalEvents);
        data[workerId][2] = String.valueOf(totalKeys);
        data[workerId][3] = String.valueOf(totalOperations);
        data[workerId][4] = String.format("%.2f", averageOperationsPerKey);
        data[workerId][5] = String.valueOf(maxOperationsPerKey);
        data[workerId][6] = String.format("%.2f", withOwnership * 100.0 / totalKeys);
        data[workerId][7] = String.format("%.2f", withoutOwnership * 100.0 / totalKeys);
    }
    private class keyStatistic extends ConcurrentHashMap<String, Integer> {
        public keyStatistic() {
            super();
        }
    }
}
