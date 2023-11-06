package intellistream.morphstream.engine.txn.durability.logging.LoggingResource.ImplLoggingResources;

import intellistream.morphstream.common.io.ByteIO.DataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.NativeDataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.SnappyDataOutputView;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.PathRecord;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResource.LoggingResources;
import intellistream.morphstream.engine.txn.durability.snapshot.LoggingOptions;
import intellistream.morphstream.util.FaultToleranceConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DependencyMaintainResources implements LoggingResources {
    private final int partitionId;
    private final PathRecord logResource;

    public DependencyMaintainResources(int partitionId, PathRecord logResource) {
        this.partitionId = partitionId;
        this.logResource = logResource;
    }

    public ByteBuffer createWriteBuffer(LoggingOptions loggingOptions) throws IOException {
        DataOutputView dataOutputView;
        if (loggingOptions.getCompressionAlg() != FaultToleranceConstants.CompressionType.None) {
            dataOutputView = new SnappyDataOutputView();//Default to use Snappy compression
        } else {
            dataOutputView = new NativeDataOutputView();
        }
        writeLogRecord(dataOutputView);
        return ByteBuffer.wrap(dataOutputView.getByteArray());
    }

    private void writeLogRecord(DataOutputView dataOutputView) throws IOException {
        dataOutputView.writeCompression(this.logResource.toString().getBytes(StandardCharsets.UTF_8));
        this.logResource.reset();
    }

}
