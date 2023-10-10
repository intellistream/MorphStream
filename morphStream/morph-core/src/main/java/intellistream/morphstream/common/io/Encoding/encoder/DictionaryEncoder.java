package intellistream.morphstream.common.io.Encoding.encoder;

import intellistream.morphstream.common.io.Enums.Encoding;
import intellistream.morphstream.common.io.Utils.Binary;
import intellistream.morphstream.common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DictionaryEncoder extends Encoder {
    private static final Logger logger = LoggerFactory.getLogger(DictionaryEncoder.class);
    private final HashMap<Binary, Integer> entryIndex;
    private final List<Binary> indexEntry;
    private final IntRleEncoder valuesEncoder;
    private long mapSize;

    public DictionaryEncoder() {
        super(Encoding.DICTIONARY);

        entryIndex = new HashMap<>();
        indexEntry = new ArrayList<>();
        valuesEncoder = new IntRleEncoder();
        mapSize = 0;
    }

    @Override
    public void encode(Binary value, ByteArrayOutputStream out) {
        entryIndex.computeIfAbsent(
                value,
                (v) -> {
                    indexEntry.add(v);
                    mapSize += v.getLength();
                    return entryIndex.size();
                });
        valuesEncoder.encode(entryIndex.get(value), out);
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        try {
            writeMap(out);
            writeEncodedData(out);
        } catch (IOException e) {
            logger.error("tsfile-encoding DictionaryEncoder: error occurs when flushing", e);
        }
        reset();
    }

    @Override
    public int getOneItemMaxSize() {
        // map + one encoded value = (map size + map value) + one encoded value = (4 + 4) + 4
        return 12;
    }

    @Override
    public long getMaxByteSize() {
        // has max size when when all points are unique
        return 4 + mapSize + valuesEncoder.getMaxByteSize();
    }

    private void writeMap(ByteArrayOutputStream out) throws IOException {
        ReadWriteForEncodingUtils.writeVarInt(indexEntry.size(), out);
        for (Binary value : indexEntry) {
            ReadWriteForEncodingUtils.writeVarInt(value.getLength(), out);
            out.write(value.getValues());
        }
    }

    private void writeEncodedData(ByteArrayOutputStream out) throws IOException {
        valuesEncoder.flush(out);
    }

    private void reset() {
        entryIndex.clear();
        indexEntry.clear();
        valuesEncoder.reset();
        mapSize = 0;
    }
}
