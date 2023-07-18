package common.io.ByteIO.OutputWithCompression;

import common.io.ByteIO.DataOutputView;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyDataOutputView extends DataOutputView {
    @Override
    public byte[] compression(byte[] in) throws IOException {
        return Snappy.compress(in);
    }

    @Override
    public void writeCompression(byte[] in) throws IOException {
        byte[] compression = compression(in);
        writeInt(compression.length);
        write(compression);
    }
}
