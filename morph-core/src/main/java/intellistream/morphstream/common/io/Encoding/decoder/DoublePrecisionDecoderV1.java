package intellistream.morphstream.common.io.Encoding.decoder;

import intellistream.morphstream.common.io.Utils.FileConfig;
import intellistream.morphstream.common.io.Utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DoublePrecisionDecoderV1 extends GorillaDecoderV1 {

    private static final Logger logger = LoggerFactory.getLogger(DoublePrecisionDecoderV1.class);
    private long preValue;

    public DoublePrecisionDecoderV1() {
        // do nothing
    }

    @Override
    public double readDouble(ByteBuffer buffer) {
        if (!flag) {
            flag = true;
            try {
                int[] buf = new int[8];
                for (int i = 0; i < 8; i++) {
                    buf[i] = ReadWriteIOUtils.read(buffer);
                }
                long res = 0L;
                for (int i = 0; i < 8; i++) {
                    res += ((long) buf[i] << (i * 8));
                }
                preValue = res;
                double tmp = Double.longBitsToDouble(preValue);
                leadingZeroNum = Long.numberOfLeadingZeros(preValue);
                tailingZeroNum = Long.numberOfTrailingZeros(preValue);
                fillBuffer(buffer);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                logger.error("DoublePrecisionDecoderV1 cannot read first double number", e);
            }
        } else {
            try {
                double tmp = Double.longBitsToDouble(preValue);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                logger.error("DoublePrecisionDecoderV1 cannot read following double number", e);
            }
        }
        return Double.NaN;
    }

    /**
     * check whether there is any value to encode left.
     *
     * @param buffer stream to read
     * @throws IOException cannot read from stream
     */
    private void getNextValue(ByteBuffer buffer) throws IOException {
        nextFlag1 = readBit(buffer);
        // case: '0'
        if (!nextFlag1) {
            return;
        }
        nextFlag2 = readBit(buffer);

        if (!nextFlag2) {
            // case: '10'
            long tmp = 0;
            for (int i = 0;
                 i < FileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNum - tailingZeroNum;
                 i++) {
                long bit = readBit(buffer) ? 1 : 0;
                tmp |= (bit << (FileConfig.VALUE_BITS_LENGTH_64BIT - 1 - leadingZeroNum - i));
            }
            tmp ^= preValue;
            preValue = tmp;
        } else {
            // case: '11'
            int leadingZeroNumTmp =
                    readIntFromStream(buffer, FileConfig.LEADING_ZERO_BITS_LENGTH_64BIT);
            int lenTmp = readIntFromStream(buffer, FileConfig.DOUBLE_VALUE_LENGTH);
            long tmp = readLongFromStream(buffer, lenTmp);
            tmp <<= (FileConfig.VALUE_BITS_LENGTH_64BIT - leadingZeroNumTmp - lenTmp);
            tmp ^= preValue;
            preValue = tmp;
        }
        leadingZeroNum = Long.numberOfLeadingZeros(preValue);
        tailingZeroNum = Long.numberOfTrailingZeros(preValue);
        if (Double.isNaN(Double.longBitsToDouble(preValue))) {
            isEnd = true;
        }
    }
}

