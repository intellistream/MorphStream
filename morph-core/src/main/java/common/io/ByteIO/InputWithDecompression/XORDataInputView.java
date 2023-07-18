package common.io.ByteIO.InputWithDecompression;

import common.io.ByteIO.DataInputView;

import java.io.IOException;
import java.nio.ByteBuffer;

public class XORDataInputView extends DataInputView {

    public XORDataInputView(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public byte[] decompression(byte[] in, int length) {
        int len = in.length;
        int key = 0x12;
        for (int i = len - 1; i > 0; i--) {
            in[i] = (byte) (in[i] ^ in[i - 1]);
        }
        in[0] = (byte) (in[0] ^ key);
        return in;
    }

    @Override
    public byte[] readFullyDecompression() throws IOException {
        int compressedLength = readInt();
        byte[] compressed = new byte[compressedLength];
        readFully(compressed);
        return decompression(compressed, compressedLength);
    }
}
