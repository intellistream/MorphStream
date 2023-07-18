package engine.txn.durability.snapshot.SnapshotStream;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;

public interface SnapshotStreamFactory {
    AsynchronousFileChannel createSnapshotStream() throws IOException;
}
