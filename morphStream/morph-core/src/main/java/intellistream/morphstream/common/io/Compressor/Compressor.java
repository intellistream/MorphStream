package intellistream.morphstream.common.io.Compressor;

public interface Compressor {
    String compress(String in);

    String uncompress(String in);
}
