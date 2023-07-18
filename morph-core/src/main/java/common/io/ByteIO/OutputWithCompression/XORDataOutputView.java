package common.io.ByteIO.OutputWithCompression;

import common.io.ByteIO.DataOutputView;

import java.io.IOException;

public class XORDataOutputView extends DataOutputView {
    @Override
    public byte[] compression(byte[] in) throws IOException {
        int len = in.length;
        int key = 0x12;
        for (int i = 0; i < len; i++) {
            in[i] = (byte) (in[i] ^ key);
            key = in[i];
        }
        return in;
    }

    @Override
    public void writeCompression(byte[] in) throws IOException {
        byte[] compression = compression(in);
        writeInt(compression.length);
        write(compression);
    }
}
