package intellistream.morphstream.api.input.statistic;

import intellistream.morphstream.common.io.Rdma.Memory.Buffer.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

/**
 *  OwnershipTable for each key
 */
public class DriverSideOwnershipTable {
    @Getter
    protected final int totalWorkers;
    protected final List<ConcurrentSkipListSet<String>> ownershipTableForEachWorker = new ArrayList<>();
    private int encoded_length;//Total size in bytes of the ownership table
    public DriverSideOwnershipTable(int totalWorkers) {
        this.totalWorkers = totalWorkers;
        for (int i = 0; i < totalWorkers; i++) {
            ownershipTableForEachWorker.add(new ConcurrentSkipListSet());
        }
    }
    public void put(String key, Integer workerId) {
        encoded_length += key.getBytes().length + 4;//Key length + Total length
        this.ownershipTableForEachWorker.get(workerId).add(key);
    }

    public boolean isWorkerOwnKey(int workerId, String key) {
        return ownershipTableForEachWorker.get(workerId).contains(key);
    }
    public void display() {
        String[] headers = {"workerId", "keyOwnershipNumber"};
        String[][] data = new String[this.ownershipTableForEachWorker.size()][2];
        int i = 0;
        for (ConcurrentSkipListSet<String> ownershipTable : ownershipTableForEachWorker) {
            data[i][0] = String.valueOf(i);
            data[i][1] = String.valueOf(ownershipTable.size());
            i++;
        }
        printAlignedBorderedTable(headers, data);
    }

    public ByteBuffer buffer() {
        //START_FLAG(Short) + TotalLength(Int) + TotalNumberForEachWorker(Int) * 4 + Length(Int) + Key + Value + Length(Int) + Key + Value + ... + END_FLAG(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(encoded_length + 2 + 4 + 4 * totalWorkers + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);
            bout.writeInt(encoded_length + 4 * totalWorkers);//Total length
            for (int i = 0; i < totalWorkers; i++) {
                bout.writeInt(ownershipTableForEachWorker.get(i).size());//TotalNumberForEachWorker
            }
            for (int i = 0; i < totalWorkers; i++) {
                for (String key : ownershipTableForEachWorker.get(i)) {
                    bout.writeInt(key.getBytes().length);
                    bout.write(key.getBytes());
                }
            }
            bout.writeShort(SOURCE_CONTROL.END_FLAG);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bout.buffer();
    }

    public void clear() {
        for (ConcurrentSkipListSet ownershipTable : ownershipTableForEachWorker) {
            ownershipTable.clear();
        }
        encoded_length = 0;
    }
}
