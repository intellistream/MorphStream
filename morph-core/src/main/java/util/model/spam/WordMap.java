package util.model.spam;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mayconbordin
 */
public class WordMap implements Serializable {
    private static final long serialVersionUID = -4598301402074863687L;
    private final Map<String, Word> words;
    private long spamTotal = 0;
    private long hamTotal = 0;

    public WordMap() {
        words = new HashMap<>();
    }

    public WordMap(Map<String, Word> words, long spamTotal, long hamTotal) {
        this.spamTotal = spamTotal;
        this.hamTotal = hamTotal;
        this.words = words;
    }

    public long getSpamTotal() {
        return spamTotal;
    }

    public long getHamTotal() {
        return hamTotal;
    }

    public void incSpamTotal(long count) {
        spamTotal += count;
    }

    public void incHamTotal(long count) {
        hamTotal += count;
    }

    public void put(String key, Word w) {
        words.put(key, w);
    }

    public Word get(String key) {
        return words.get(key);
    }

    public boolean containsKey(String key) {
        return words.containsKey(key);
    }

    public Collection<Word> values() {
        return words.values();
    }

    public static class WordMapSerializer extends Serializer<WordMap> {
        @Override
        public void write(Kryo kryo, Output output, WordMap object) {
            kryo.writeObject(output, object.words);
            output.writeLong(object.spamTotal);
            output.writeLong(object.hamTotal);
        }

        @Override
        public WordMap read(Kryo kryo, Input input, Class<WordMap> type) {
            return new WordMap(kryo.readObject(input, HashMap.class), input.readLong(), input.readLong());
        }
    }
}