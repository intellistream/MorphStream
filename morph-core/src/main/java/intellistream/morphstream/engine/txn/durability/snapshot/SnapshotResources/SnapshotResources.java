package intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResources;

/**
 * Performs the synchronous part of the snapshot. It returns resources which can be later
 * on used in the asynchronous
 * Created by curry at 2022/12/17
 */
public interface SnapshotResources {

    /**
     * Cleans up the resources after the asynchronous part is done.
     */
    void release();
}
