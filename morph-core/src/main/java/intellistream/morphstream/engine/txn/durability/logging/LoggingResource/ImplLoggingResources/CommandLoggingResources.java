package intellistream.morphstream.engine.txn.durability.logging.LoggingResource.ImplLoggingResources;

import intellistream.morphstream.common.io.ByteIO.DataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.NativeDataOutputView;
import intellistream.morphstream.common.io.ByteIO.OutputWithCompression.SnappyDataOutputView;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResource.LoggingResources;
import intellistream.morphstream.engine.txn.durability.snapshot.LoggingOptions;
import intellistream.morphstream.engine.txn.durability.struct.Logging.NativeCommandLog;
import intellistream.morphstream.util.FaultToleranceConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Vector;

public class CommandLoggingResources implements LoggingResources {
    private final int partitionId;
    private final Vector<NativeCommandLog> commandLogs;

    public CommandLoggingResources(int partitionId, Vector<NativeCommandLog> commandLogs) {
        this.partitionId = partitionId;
        this.commandLogs = commandLogs;
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
        //IOUtils.println("Partition " + partitionId + " has " + commandLogs.size() + " command logs");
        for (NativeCommandLog nativeCommandLog : commandLogs) {
            if (!nativeCommandLog.isAborted)
                stringBuilder.append(nativeCommandLog).append(" ");
        }
        dataOutputView.writeCompression(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        this.commandLogs.clear();
    }
}
