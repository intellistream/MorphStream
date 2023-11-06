package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResources;

import intellistream.morphstream.engine.txn.storage.table.RecordSchema;

import java.io.Serializable;

public class StateMetaInfoSnapshot implements Serializable {
    public final RecordSchema recordSchema;
    public final String tableName;
    public final int partitionId;
    public int recordNum;

    public StateMetaInfoSnapshot(RecordSchema recordSchema, String tableName, int partitionId) {
        this.recordSchema = recordSchema;
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public void setRecordNum(int recordNum) {
        this.recordNum = recordNum;
    }
}
