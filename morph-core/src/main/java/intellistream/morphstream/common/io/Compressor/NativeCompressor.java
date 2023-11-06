package intellistream.morphstream.common.io.Compressor;

public class NativeCompressor implements Compressor {
    @Override
    public String compress(String in) {
        return in;
    }

    @Override
    public String uncompress(String in) {
        return in;
    }
}
