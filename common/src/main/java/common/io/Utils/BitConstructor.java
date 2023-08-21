package common.io.Utils;

public class BitConstructor{
        private static final int BITS_IN_A_BYTE = 8;
        private static final long ALL_MASK = -1;
        private final ByteArrayList data;
        private byte cache = 0;
        private int cnt = 0;

        public BitConstructor() {
            this.data = new ByteArrayList();
        }

        public BitConstructor(int initialCapacity) {
            this.data = new ByteArrayList(initialCapacity);
        }

        public void add(long x, int len) {
            x = x & ~(ALL_MASK << len); // Make sure that all bits expect the lowest len bits of x are 0
            while (len > 0) {
                // Number of bits inserted into cache
                int m = len + cnt >= BITS_IN_A_BYTE ? BITS_IN_A_BYTE - cnt : len;
                len -= m;
                cnt += m;
                byte y = (byte) (x >> len);
                y = (byte) (y << (BITS_IN_A_BYTE - cnt));
                cache = (byte) (cache | y);
                x = x & ~(ALL_MASK << len);
                if (cnt == BITS_IN_A_BYTE) {
                    pad();
                }
            }
        }

        public byte[] toByteArray() {
            byte[] ret;
            if (cnt > 0) {
                data.add(cache);
                ret = data.toArray();
                data.removeAtIndex(data.size() - 1);
            } else {
                ret = data.toArray();
            }
            return ret;
        }

        public void clear() {
            data.clear();
            cache = 0x00;
            cnt = 0;
        }

        /** Fill the rest part of cache with 0 */
        public void pad() {
            if (cnt > 0) {
                data.add(cache);
                cache = 0x00;
                cnt = 0;
            }
        }

        public void add(byte[] bytes) {
            if (cnt == 0) {
                data.addAll(bytes);
            } else {
                for (byte aByte : bytes) {
                    add(aByte, 8);
                }
            }
        }

        public int sizeInBytes() {
            return data.size() + (cnt > 0 ? 1 : 0);
        }
}

