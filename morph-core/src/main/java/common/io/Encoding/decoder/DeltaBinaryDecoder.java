package common.io.Encoding.decoder;

import common.io.Enums.Encoding;
import common.io.Utils.BytesUtils;
import common.io.Utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a decoder for decoding the byte array that encoded by {@code
 * DeltaBinaryEncoder}.DeltaBinaryDecoder just supports integer and long values.<br>
 * .
 *
 * @see common.io.Encoding.encoder.DeltaBinaryEncoder
 */
public abstract class DeltaBinaryDecoder extends Decoder{

    protected long count = 0;
    protected byte[] deltaBuf;

    /** the first value in one pack. */
    protected int readIntTotalCount = 0;

    protected int nextReadIndex = 0;
    /** max bit length of all value in a pack. */
    protected int packWidth;
    /** data number in this pack. */
    protected int packNum;

    /** how many bytes data takes after encoding. */
    protected int encodingLength;

    public DeltaBinaryDecoder() {
        super(Encoding.TS_2DIFF);
    }

    protected abstract void readHeader(ByteBuffer buffer) throws IOException;

    protected abstract void allocateDataArray();

    protected abstract void readValue(int i);

    /**
     * calculate the bytes length containing v bits.
     *
     * @param v - number of bits
     * @return number of bytes
     */
    protected int ceil(int v) {
        return (int) Math.ceil((double) (v) / 8.0);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
    }

    public static class IntDeltaDecoder extends DeltaBinaryDecoder {

        private int firstValue;
        private int[] data;
        private int previous;
        /** minimum value for all difference. */
        private int minDeltaBase;

        public IntDeltaDecoder() {
            super();
        }

        /**
         * if there's no decoded data left, decode next pack into {@code data}.
         *
         * @param buffer ByteBuffer
         * @return int
         */
        protected int readT(ByteBuffer buffer) {
            if (nextReadIndex == readIntTotalCount) {
                return loadIntBatch(buffer);
            }
            return data[nextReadIndex++];
        }

        @Override
        public int readInt(ByteBuffer buffer) {
            return readT(buffer);
        }

        /**
         * if remaining data has been run out, load next pack from InputStream.
         *
         * @param buffer ByteBuffer
         * @return int
         */
        protected int loadIntBatch(ByteBuffer buffer) {
            packNum = ReadWriteIOUtils.readInt(buffer);
            packWidth = ReadWriteIOUtils.readInt(buffer);
            count++;
            readHeader(buffer);

            encodingLength = ceil(packNum * packWidth);
            deltaBuf = new byte[encodingLength];
            buffer.get(deltaBuf);
            allocateDataArray();

            previous = firstValue;
            readIntTotalCount = packNum;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }

        private void readPack() {
            for (int i = 0; i < packNum; i++) {
                readValue(i);
                previous = data[i];
            }
        }

        @Override
        protected void readHeader(ByteBuffer buffer) {
            minDeltaBase = ReadWriteIOUtils.readInt(buffer);
            firstValue = ReadWriteIOUtils.readInt(buffer);
        }

        @Override
        protected void allocateDataArray() {
            data = new int[packNum];
        }

        @Override
        protected void readValue(int i) {
            int v = BytesUtils.bytesToInt(deltaBuf, packWidth * i, packWidth);
            data[i] = previous + minDeltaBase + v;
        }

        @Override
        public void reset() {
            // do nothing
        }
    }

    public static class LongDeltaDecoder extends DeltaBinaryDecoder {

        private long firstValue;
        private long[] data;
        private long previous;
        /** minimum value for all difference. */
        private long minDeltaBase;

        public LongDeltaDecoder() {
            super();
        }

        /**
         * if there's no decoded data left, decode next pack into {@code data}.
         *
         * @param buffer ByteBuffer
         * @return long value
         */
        protected long readT(ByteBuffer buffer) {
            if (nextReadIndex == readIntTotalCount) {
                return loadIntBatch(buffer);
            }
            return data[nextReadIndex++];
        }

        /**
         * if remaining data has been run out, load next pack from InputStream.
         *
         * @param buffer ByteBuffer
         * @return long value
         */
        protected long loadIntBatch(ByteBuffer buffer) {
            packNum = ReadWriteIOUtils.readInt(buffer);
            packWidth = ReadWriteIOUtils.readInt(buffer);
            count++;
            readHeader(buffer);

            encodingLength = ceil(packNum * packWidth);
            deltaBuf = new byte[encodingLength];
            buffer.get(deltaBuf);
            allocateDataArray();

            previous = firstValue;
            readIntTotalCount = packNum;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }

        private void readPack() {
            for (int i = 0; i < packNum; i++) {
                readValue(i);
                previous = data[i];
            }
        }

        @Override
        public long readLong(ByteBuffer buffer) {

            return readT(buffer);
        }

        @Override
        protected void readHeader(ByteBuffer buffer) {
            minDeltaBase = ReadWriteIOUtils.readLong(buffer);
            firstValue = ReadWriteIOUtils.readLong(buffer);
        }

        @Override
        protected void allocateDataArray() {
            data = new long[packNum];
        }

        @Override
        protected void readValue(int i) {
            long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
            data[i] = previous + minDeltaBase + v;
        }

        @Override
        public void reset() {
            // do nothing
        }
    }
}
