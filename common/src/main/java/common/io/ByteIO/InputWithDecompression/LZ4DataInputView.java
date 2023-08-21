package common.io.ByteIO.InputWithDecompression;

import common.io.ByteIO.DataInputView;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LZ4DataInputView extends DataInputView {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4FastDecompressor decompressor = factory.fastDecompressor();
    public LZ4DataInputView(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public byte[] decompression(byte[] in, int length) {
        return new byte[0];
    }

    @Override
    public byte[] readFullyDecompression() throws IOException {
        int decompressedLength = readInt();
        int compressedLength = readInt();
        byte[] compressed = new byte[compressedLength];
        readFully(compressed);
        return decompressor.decompress(compressed, decompressedLength);
    }
}
