package common.io.Encoding.decoder;

import common.io.Utils.FileConfig;
import common.io.Utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SinglePrecisionDecoderV1 extends GorillaDecoderV1 {

    private static final Logger logger = LoggerFactory.getLogger(SinglePrecisionDecoderV1.class);
    private int preValue;

    public SinglePrecisionDecoderV1() {
        // do nothing
    }

    @Override
    public float readFloat(ByteBuffer buffer) {
        if (!flag) {
            flag = true;
            try {
                int ch1 = ReadWriteIOUtils.read(buffer);
                int ch2 = ReadWriteIOUtils.read(buffer);
                int ch3 = ReadWriteIOUtils.read(buffer);
                int ch4 = ReadWriteIOUtils.read(buffer);
                preValue = ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
                leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
                tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
                float tmp = Float.intBitsToFloat(preValue);
                fillBuffer(buffer);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                logger.error("SinglePrecisionDecoderV1 cannot read first float number", e);
            }
        } else {
            try {
                float tmp = Float.intBitsToFloat(preValue);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                logger.error("SinglePrecisionDecoderV1 cannot read following float number", e);
            }
        }
        return Float.NaN;
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
            // case: '10';
            int tmp = 0;
            for (int i = 0;
                 i < FileConfig.VALUE_BITS_LENGTH_32BIT - leadingZeroNum - tailingZeroNum;
                 i++) {
                int bit = readBit(buffer) ? 1 : 0;
                tmp |= bit << (FileConfig.VALUE_BITS_LENGTH_32BIT - 1 - leadingZeroNum - i);
            }
            tmp ^= preValue;
            preValue = tmp;
        } else {
            // case: '11';
            int leadingZeroNumTmp =
                    readIntFromStream(buffer, FileConfig.LEADING_ZERO_BITS_LENGTH_32BIT);
            int lenTmp = readIntFromStream(buffer, FileConfig.FLOAT_VALUE_LENGTH);
            int tmp = readIntFromStream(buffer, lenTmp);
            tmp <<= (FileConfig.VALUE_BITS_LENGTH_32BIT - leadingZeroNumTmp - lenTmp);
            tmp ^= preValue;
            preValue = tmp;
        }
        leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
        tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
        if (Float.isNaN(Float.intBitsToFloat(preValue))) {
            isEnd = true;
        }
    }
}
