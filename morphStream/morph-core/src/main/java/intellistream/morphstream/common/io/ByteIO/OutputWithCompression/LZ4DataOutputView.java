package intellistream.morphstream.common.io.ByteIO.OutputWithCompression;

import intellistream.morphstream.common.io.ByteIO.DataOutputView;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;


public class LZ4DataOutputView extends DataOutputView {
    LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    LZ4Compressor compressor = lz4Factory.fastCompressor();

    public byte[] compression(byte[] in) {
        int decompressedLength = in.length;
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(in, 0, decompressedLength, compressed, 0, maxCompressedLength);
        return compressed;
    }

    @Override
    public synchronized void writeCompression(byte[] in) throws IOException {
        writeInt(in.length);
        byte[] compressionByte = compression(in);
        writeInt(compressionByte.length);
        write(compressionByte);
    }
}
