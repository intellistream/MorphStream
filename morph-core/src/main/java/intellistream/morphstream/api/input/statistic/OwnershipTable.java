package intellistream.morphstream.api.input.statistic;

import intellistream.morphstream.common.io.Rdma.Memory.Buffer.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

/**
 *  OwnershipTable for each key
 */
public class OwnershipTable extends HashMap<String, Integer> {
    private int encoded_length;//Total size in bytes of the ownership table
    @Override
    public Integer put(String key, Integer value) {
        encoded_length += key.length() + 4 + 4;//Key length + Value length + Total length
        return super.put(key, value);
    }
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

    public ByteBuffer buffer() {
        //START_FLAG(Short) + TotalLength(Int) + TotalSize(Int) + Length(Int) + Key + Value + Length(Int) + Key + Value + ... + END_FLAG(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(encoded_length + 2 + 4 + 4 + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);
            bout.writeInt(encoded_length + 4);//Total length
            bout.writeInt(this.size());//Total size
            for (Map.Entry<String, Integer> entry : this.entrySet()) {
                bout.writeInt(entry.getKey().length());
                bout.write(entry.getKey().getBytes());
                bout.writeInt(entry.getValue());
            }
            bout.writeShort(SOURCE_CONTROL.END_FLAG);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bout.buffer();
    }

    @Override
    public void clear() {
        super.clear();
        encoded_length = 0;
    }
}
