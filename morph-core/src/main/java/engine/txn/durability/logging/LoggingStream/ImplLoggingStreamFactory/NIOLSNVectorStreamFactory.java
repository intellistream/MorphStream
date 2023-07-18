package engine.txn.durability.logging.LoggingStream.ImplLoggingStreamFactory;

import common.collections.OsUtils;
import engine.txn.durability.logging.LoggingStream.LoggingStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class NIOLSNVectorStreamFactory implements LoggingStreamFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOLSNVectorStreamFactory.class);
    private final Path logPath;
    public NIOLSNVectorStreamFactory(String walPath) {
        String filePath = walPath + OsUtils.OS_wrapper(UUID.randomUUID() + ".wal");
        this.logPath = Paths.get(filePath);
    }
    @Override
    public AsynchronousFileChannel createLoggingStream() throws IOException {
        return AsynchronousFileChannel.open(logPath, WRITE, CREATE);
    }

    public Path getPath() {
        return logPath;
    }
}
