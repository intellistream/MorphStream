package durability.snapshot.SnapshotStream.ImplSnapshotStreamFactory;

import common.collections.OsUtils;
import durability.snapshot.SnapshotStream.SnapshotStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FaultToleranceConstants;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class NIOSnapshotStreamFactory implements SnapshotStreamFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOSnapshotStreamFactory.class);
    private final Path snapshotPath;
    private final FaultToleranceConstants.CompressionType compressionType;

    public NIOSnapshotStreamFactory(String snapshotPath, FaultToleranceConstants.CompressionType compressionType) {
        String filePath = snapshotPath + OsUtils.OS_wrapper(UUID.randomUUID() + ".snapshot");
        this.snapshotPath = Paths.get(filePath);
        this.compressionType = compressionType;
    }

    @Override
    public AsynchronousFileChannel createSnapshotStream() throws IOException {
        return AsynchronousFileChannel.open(snapshotPath, WRITE, CREATE);
    }

    public Path getSnapshotPath() {
        return snapshotPath;
    }
}
