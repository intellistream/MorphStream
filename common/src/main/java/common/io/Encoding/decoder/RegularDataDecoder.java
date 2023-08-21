package common.io.Encoding.decoder;

import common.io.Enums.Encoding;
import common.io.Utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

public abstract class RegularDataDecoder extends Decoder {

    /** the first value in one pack. */
    protected int readIntTotalCount = 0;

    protected int nextReadIndex = 0;
    /** data number in this pack. */
    protected int packNum;

    public RegularDataDecoder() {
        super(Encoding.REGULAR);
    }

    protected abstract void readHeader(ByteBuffer buffer) throws IOException;

    protected abstract void allocateDataArray();

    protected abstract void readValue(int i);

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        return (nextReadIndex < readIntTotalCount) || buffer.remaining() > 0;
    }

    public static class IntRegularDecoder extends RegularDataDecoder {

        private int[] data;
        private int firstValue;
        private int previous;
        private boolean isMissingPoint;
        private BitSet bitmap;
        private int bitmapIndex;
        /** minimum value for all difference. */
        private int minDeltaBase;

        public IntRegularDecoder() {
            super();
        }

        /**
         * if there's no decoded data left, decode next pack into {@code data}.
         *
         * @param buffer ByteBuffer
         * @return long value
         */
        protected int readT(ByteBuffer buffer) {
            if (nextReadIndex == readIntTotalCount) {
                isMissingPoint = ReadWriteIOUtils.readBool(buffer);
                if (isMissingPoint) {
                    readBitmap(buffer);
                }
                return loadIntBatch(buffer); // load first value
            }
            if (isMissingPoint) {
                bitmapIndex++;
                return loadWithBitmap(buffer);
            }
            return data[nextReadIndex++];
        }

        private void readBitmap(ByteBuffer buffer) {
            int length = ReadWriteIOUtils.readInt(buffer);
            byte[] byteArr = new byte[length];
            buffer.get(byteArr);
            bitmap = BitSet.valueOf(byteArr);
            bitmapIndex = 0;
        }

        /**
         * load the data with bitmap (when bitmap denote the element with false, load next element)
         *
         * @param buffer
         * @return long value
         */
        protected int loadWithBitmap(ByteBuffer buffer) {
            while (!bitmap.get(bitmapIndex)) {
                bitmapIndex++;
            }
            nextReadIndex = bitmapIndex - 1;
            return data[nextReadIndex];
        }

        /**
         * if remaining data has been run out, load next pack from InputStream.
         *
         * @param buffer ByteBuffer
         * @return int value
         */
        protected int loadIntBatch(ByteBuffer buffer) {
            packNum = ReadWriteIOUtils.readInt(buffer);
            readHeader(buffer);

            allocateDataArray();

            readIntTotalCount = isMissingPoint ? (packNum - 2) : (packNum - 1);
            previous = firstValue;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }

        private void readPack() {
            for (int i = 0; i < data.length; i++) {
                readValue(i);
                previous = data[i];
            }
        }

        @Override
        public int readInt(ByteBuffer buffer) {
            return readT(buffer);
        }

        @Override
        protected void readHeader(ByteBuffer buffer) {
            minDeltaBase = ReadWriteIOUtils.readInt(buffer);
            firstValue = ReadWriteIOUtils.readInt(buffer);
        }

        @Override
        protected void allocateDataArray() {
            data = new int[packNum - 1];
        }

        @Override
        protected void readValue(int i) {
            data[i] = previous + minDeltaBase;
        }

        @Override
        public void reset() {
            // do nothing
        }
    }

    public static class LongRegularDecoder extends RegularDataDecoder {

        private long[] data;
        private long firstValue;
        private long previous;
        private boolean isMissingPoint;
        private BitSet bitmap;
        private int bitmapIndex;
        /** minimum value for all difference. */
        private long minDeltaBase;

        public LongRegularDecoder() {
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
                isMissingPoint = ReadWriteIOUtils.readBool(buffer);
                if (isMissingPoint) {
                    readBitmap(buffer);
                }
                return loadIntBatch(buffer); // load first value
            }
            if (isMissingPoint) {
                bitmapIndex++;
                return loadWithBitmap(buffer);
            }
            return data[nextReadIndex++];
        }

        private void readBitmap(ByteBuffer buffer) {
            int length = ReadWriteIOUtils.readInt(buffer);
            byte[] byteArr = new byte[length];
            buffer.get(byteArr);
            bitmap = BitSet.valueOf(byteArr);
            bitmapIndex = 0;
        }

        /**
         * load the data with bitmap (when bitmap denote the element with false, load next element)
         *
         * @param buffer
         * @return long value
         */
        protected long loadWithBitmap(ByteBuffer buffer) {
            while (!bitmap.get(bitmapIndex)) {
                bitmapIndex++;
            }
            nextReadIndex = bitmapIndex - 1;
            return data[nextReadIndex];
        }

        /**
         * if remaining data has been run out, load next pack from InputStream.
         *
         * @param buffer ByteBuffer
         * @return long value
         */
        protected long loadIntBatch(ByteBuffer buffer) {
            packNum = ReadWriteIOUtils.readInt(buffer);
            readHeader(buffer);

            allocateDataArray();

            readIntTotalCount = isMissingPoint ? (packNum - 2) : (packNum - 1);
            previous = firstValue;
            nextReadIndex = 0;
            readPack();
            return firstValue;
        }

        private void readPack() {
            for (int i = 0; i < data.length; i++) {
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
            data = new long[packNum - 1];
        }

        @Override
        protected void readValue(int i) {
            data[i] = previous + minDeltaBase;
        }

        @Override
        public void reset() {
            // do nothing
        }
    }
}
