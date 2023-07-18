package intellistream.morphstream.common.io.LocalFS;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

public class LocalDataInputStream extends InputStream {
    /**
     * The file input stream used to read data from.
     */
    private final FileInputStream fis;

    private final FileChannel fileChannel;

    /**
     * Constructs a new <code>LocalDataInputStream</code> object from a given {@link File} object.
     *
     * @param file The File the data stream is read from
     * @throws IOException Thrown if the data input stream cannot be created.
     */
    public LocalDataInputStream(File file) throws IOException {
        this.fis = new FileInputStream(file);
        this.fileChannel = fis.getChannel();
    }

    public void seek(long desired) throws IOException {
        if (desired != getPos()) {
            this.fileChannel.position(desired);
        }
    }

    public long getPos() throws IOException {
        return this.fileChannel.position();
    }

    @Override
    public int read() throws IOException {
        return this.fis.read();
    }

    @Override
    public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
        return this.fis.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
        // According to javadoc, this also closes the channel
        this.fis.close();
    }

    @Override
    public int available() throws IOException {
        return this.fis.available();
    }

    @Override
    public long skip(final long n) throws IOException {
        return this.fis.skip(n);
    }
}
