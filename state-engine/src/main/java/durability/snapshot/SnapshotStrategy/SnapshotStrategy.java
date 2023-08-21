package durability.snapshot.SnapshotStrategy;

import durability.ftmanager.FTManager;
import durability.snapshot.SnapshotResources.SnapshotResources;
import storage.table.RecordSchema;

import java.io.IOException;

public interface SnapshotStrategy<SR extends SnapshotResources> {

    /**
     * Performs the synchronous part of the snapshot. It returns resources which can be later
     * on used in the asynchronous
     * @param snapshotId the ID of the shapshot
     * @return Resources needed to finish the snapshot
     * @throws Exception
     */
    SR syncPrepareResources(long snapshotId, int partitionId);
    void registerTable(String tableName, RecordSchema r);
    void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException;
    String getDescription();
}
