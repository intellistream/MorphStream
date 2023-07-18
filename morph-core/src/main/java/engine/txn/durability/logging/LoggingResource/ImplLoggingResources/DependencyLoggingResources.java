package engine.txn.durability.logging.LoggingResource.ImplLoggingResources;

import common.io.ByteIO.DataOutputView;
import common.io.ByteIO.OutputWithCompression.NativeDataOutputView;
import common.io.ByteIO.OutputWithCompression.SnappyDataOutputView;
import engine.txn.durability.logging.LoggingResource.LoggingResources;
import engine.txn.durability.snapshot.LoggingOptions;
import engine.txn.durability.struct.Logging.DependencyLog;
import util.FaultToleranceConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Vector;

public class DependencyLoggingResources implements LoggingResources {
    private final int partitionId;
    private final Vector<DependencyLog> dependencyLogs;

    public DependencyLoggingResources(int partitionId, Vector<DependencyLog> dependencyLogs) {
        this.partitionId = partitionId;
        this.dependencyLogs = dependencyLogs;
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
        StringBuilder stringBuilder = new StringBuilder();
        //IOUtils.println("Partition " + partitionId + " has " + dependencyLogs.size() + " dependency logs");
        for (DependencyLog dependencyLog : dependencyLogs) {
            stringBuilder.append(dependencyLog.toString()).append(" ");
        }
        dataOutputView.writeCompression(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        this.dependencyLogs.clear();
    }
}
