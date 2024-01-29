package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResources.ImplSnapshotResources;

import intellistream.morphstream.common.io.ByteIO.DataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.NativeDataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.SnappyDataOutputView;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotOptions;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResources.SnapshotResources;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResources.StateMetaInfoSnapshot;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy.InMemorySnapshotStrategy;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.BaseTable;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.Serialize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class InMemoryFullSnapshotResources implements SnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();
    //<TableName, <Key, TableRecord>>
    private final HashMap<String, HashMap<String, TableRecord>> snapshotResource = new HashMap<>();
    private final long snapshotId;
    private final int partitionId;

    public InMemoryFullSnapshotResources(long snapshotId, int partitionId, Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation, Map<String, BaseTable> tables) {
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        createSnapshotResources(tables);
        createStateMetaInfoSnapshot(kvStateInformation);
    }

    private void createStateMetaInfoSnapshot(Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation) {
        for (InMemorySnapshotStrategy.InMemoryKvStateInfo info : kvStateInformation.values()) {
            StateMetaInfoSnapshot stateMetaInfoSnapshot = new StateMetaInfoSnapshot(info.recordSchema, info.tableName, this.partitionId);
            stateMetaInfoSnapshot.setRecordNum(snapshotResource.get(info.tableName).size());
            this.stateMetaInfoSnapshots.add(stateMetaInfoSnapshot);
        }
    }

    private void createSnapshotResources(Map<String, BaseTable> tables) {
        for (Map.Entry<String, BaseTable> table : tables.entrySet()) {
            snapshotResource.put(table.getKey(), table.getValue().getTableIndexByPartitionId(this.partitionId));
        }
    }

    public ByteBuffer createWriteBuffer(SnapshotOptions snapshotOptions) throws IOException {
        DataOutputView dataOutputView;
        if (snapshotOptions.getCompressionAlg() != FaultToleranceConstants.CompressionType.None) {
            dataOutputView = new SnappyDataOutputView();//Default to use Snappy compression
        } else {
            dataOutputView = new NativeDataOutputView();
        }
        writeKVStateMetaData(dataOutputView);
        writeKVStateDate(dataOutputView);
        return ByteBuffer.wrap(dataOutputView.getByteArray());
    }

    private void writeKVStateMetaData(DataOutputView dataOutputView) throws IOException {
        dataOutputView.writeInt(this.stateMetaInfoSnapshots.size());
        List<byte[]> objects = new ArrayList<>();
        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : this.stateMetaInfoSnapshots) {
            objects.add(Serialize.serializeObject(stateMetaInfoSnapshot));
        }
        for (byte[] o : objects) {
            dataOutputView.writeCompression(o);
        }
    }

    private void writeKVStateDate(DataOutputView dataOutputView) throws IOException {
        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            HashMap<String, TableRecord> tables = snapshotResource.get(stateMetaInfoSnapshot.tableName);
            Iterator<TableRecord> recordIterator = tables.values().iterator();
            while (recordIterator.hasNext()) {
                TableRecord tableRecord = recordIterator.next();
                String str = tableRecord.toSerializableString(this.snapshotId);
                dataOutputView.writeCompression(str.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Override
    public void release() {

    }
}
