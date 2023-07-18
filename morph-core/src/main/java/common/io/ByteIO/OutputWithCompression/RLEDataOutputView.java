package common.io.ByteIO.OutputWithCompression;

import common.io.ByteIO.DataOutputView;

import java.io.IOException;

public class RLEDataOutputView extends DataOutputView {
    @Override
    public byte[] compression(byte[] in) {
        return in;
    }

    @Override
    public synchronized void writeCompression(byte[] in) throws IOException {
        writeInt(in.length);
        write(in);
    }
}
