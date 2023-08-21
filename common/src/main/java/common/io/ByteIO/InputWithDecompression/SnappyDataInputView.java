package common.io.ByteIO.InputWithDecompression;

import common.io.ByteIO.DataInputView;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyDataInputView extends DataInputView {
    public SnappyDataInputView(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public byte[] decompression(byte[] in, int length) {
        return new byte[0];
    }

    @Override
    public byte[] readFullyDecompression() throws IOException {
        int compressedLength = readInt();
        byte[] compressed = new byte[compressedLength];
        readFully(compressed);
        return Snappy.uncompress(compressed);
    }
}
