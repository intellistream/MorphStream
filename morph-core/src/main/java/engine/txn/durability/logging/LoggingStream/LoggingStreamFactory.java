package engine.txn.durability.logging.LoggingStream;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;

public interface LoggingStreamFactory {
    AsynchronousFileChannel createLoggingStream() throws IOException;
}
