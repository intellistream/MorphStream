package engine.txn.durability.logging.LoggingResource.ImplLoggingResources;

import common.io.ByteIO.DataOutputView;
import common.io.ByteIO.OutputWithCompression.NativeDataOutputView;
import common.io.ByteIO.OutputWithCompression.SnappyDataOutputView;
import engine.txn.durability.logging.LoggingEntry.LVLogRecord;
import engine.txn.durability.logging.LoggingResource.LoggingResources;
import engine.txn.durability.snapshot.LoggingOptions;
import util.FaultToleranceConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LSNVectorLoggingResources implements LoggingResources{
    private final int partitionId;
    private final LVLogRecord logResource;
    public LSNVectorLoggingResources(int partitionId, LVLogRecord logResource) {
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
        this.logResource.clean();
    }

}
