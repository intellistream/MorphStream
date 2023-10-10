package intellistream.morphstream.common.io.Compressor;


import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SnappyCompressor implements Compressor {
    @Override
    public String compress(String in) {
        try {
            return new String(Snappy.compress(in), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String uncompress(String in) {
        byte[] dataBytes = in.getBytes();
        try {
            return new String(Snappy.uncompress(dataBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
