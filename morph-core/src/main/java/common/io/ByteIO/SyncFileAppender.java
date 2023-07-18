package common.io.ByteIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.*;

/**
 * File Appender using JDK 7's AsynchronousFileChannel
 */
public class SyncFileAppender {
    protected boolean append = true;
    protected Path path;
    protected AsynchronousFileChannel fileChannel;
    protected AtomicLong position = new AtomicLong(0);
    public SyncFileAppender(boolean append, Path path) {
        this.append = append;
        this.path = path;
        createFileChannel();
    }
    protected AsynchronousFileChannel createFileChannel() {
        if (null == this.fileChannel) {
            try {
                if (!append) {
                    this.fileChannel = AsynchronousFileChannel.open(this.path, CREATE, WRITE, TRUNCATE_EXISTING);
                } else {
                    this.fileChannel = AsynchronousFileChannel.open(this.path, CREATE, WRITE);
                }
                if (append) {
                    this.position.set(this.fileChannel.size());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return fileChannel;
    }
    public void append(ByteBuffer byteBuffer) throws IOException, ExecutionException, InterruptedException {
        AsynchronousFileChannel fileChannel = createFileChannel();
        if (null == fileChannel) {
            throw new IOException("No file set for path: " + path);
        }

        Future<Integer> result = fileChannel.write(byteBuffer, this.position.get());
        this.position.getAndAdd(result.get());
    }

    public void close() throws IOException, InterruptedException {
        if (null != this.fileChannel) {
            fileChannel.close();
        }
    }

    public AtomicLong getPosition() {
        return position;
    }
}
