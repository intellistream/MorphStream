package intellistream.morphstream.common.io.Encoding.decoder;

import intellistream.morphstream.common.io.Enums.Encoding;
import intellistream.morphstream.common.io.Utils.Binary;
import intellistream.morphstream.common.io.Utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DictionaryDecoder extends Decoder {
    private static final Logger logger = LoggerFactory.getLogger(DictionaryDecoder.class);
    private final IntRleDecoder valueDecoder;
    private List<Binary> entryIndex;

    public DictionaryDecoder() {
        super(Encoding.DICTIONARY);

        valueDecoder = new IntRleDecoder();
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        if (entryIndex == null) {
            initMap(buffer);
        }

        try {
            return valueDecoder.hasNext(buffer);
        } catch (IOException e) {
            logger.error("tsfile-decoding DictionaryDecoder: error occurs when decoding", e);
        }

        return false;
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        if (entryIndex == null) {
            initMap(buffer);
        }
        int code = valueDecoder.readInt(buffer);
        return entryIndex.get(code);
    }

    private void initMap(ByteBuffer buffer) {
        int length = ReadWriteForEncodingUtils.readVarInt(buffer);
        entryIndex = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            int binaryLength = ReadWriteForEncodingUtils.readVarInt(buffer);
            byte[] buf = new byte[binaryLength];
            buffer.get(buf, 0, binaryLength);
            entryIndex.add(new Binary(buf));
        }
    }

    @Override
    public void reset() {
        entryIndex = null;
        valueDecoder.reset();
    }
}
