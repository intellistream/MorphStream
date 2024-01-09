package intellistream.morphstream.api.input.statistic;

import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

/**
 *  OwnershipTable for each key
 */
public class OwnershipTable extends HashMap<String, Integer> {
    public void display() {
        String[] headers = {"workerId", "keyOwnershipNumber"};
        String[][] data = new String[countKeysByValue(this).size()][2];
        int i = 0;
        for (Map.Entry<Integer, Integer> entry : countKeysByValue(this).entrySet()) {
            data[i][0] = String.valueOf(entry.getKey());
            data[i][1] = String.valueOf(entry.getValue());
            i++;
        }
        printAlignedBorderedTable(headers, data);
    }
    public HashMap<Integer, Integer> countKeysByValue(HashMap<String, Integer> inputMap) {
        HashMap<Integer, Integer> countMap = new HashMap<>();

        for (int value : inputMap.values()) {
            countMap.put(value, countMap.getOrDefault(value, 0) + 1);
        }

        return countMap;
    }
}
