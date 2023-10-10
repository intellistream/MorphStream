package intellistream.morphstream.common.io.ByteIO.InputWithDecompression;

import intellistream.morphstream.common.io.ByteIO.DataInputView;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RLEDataInputView extends DataInputView {
    public RLEDataInputView(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public byte[] decompression(byte[] in, int length) {
        return in;
    }

    @Override
    public byte[] readFullyDecompression() throws IOException {
        byte[] b = new byte[readInt()];
        readFully(b);
        return b;
    }
}
