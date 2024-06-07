package intellistream.morphstream.common.io.RDMA;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RDMAAtomicTest extends TestCase {
    public RDMAAtomicTest(String testName) {
        super(testName);
    }

    public void testCAS() {
        if (ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN)) {
            System.out.println("Native order is little endian");
        } else {
            System.out.println("Native order is big endian");
        }
        assertTrue(true);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        long bid = 1480;
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        byteBuffer.putLong(bid);
        byteBuffer.flip();
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(8);
        byteBuffer1.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer1.putLong(1480);
        byteBuffer1.flip();
        int first = byteBuffer1.getInt();
        int second = byteBuffer1.getInt();
        System.out.println("First short: " + first);
        System.out.println("Second short: " + second);
    }
    public void testFAA() {
        assertTrue(true);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        long bid = 1480;
        byteBuffer.putLong(bid);
        byteBuffer.flip();
        int first = byteBuffer.getInt();
        int second = byteBuffer.getInt();
        System.out.println("First int: " + first);
        System.out.println("Second int: " + second);
        byteBuffer.flip();
        long before = byteBuffer.getLong();
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(8);
        byteBuffer1.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer1.putLong(before + (1L << 32));
        byteBuffer1.flip();
        int first1 = byteBuffer1.getInt();
        int second1 = byteBuffer1.getInt();
        System.out.println("First int: " + first1);
        System.out.println("Second int: " + second1);
        byteBuffer1.flip();
        before = byteBuffer1.getLong();
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(8);
        byteBuffer2.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer2.putLong(before + (-1L << 32));
        byteBuffer2.flip();
        int first2 = byteBuffer2.getInt();
        int second2 = byteBuffer2.getInt();
        System.out.println("First int: " + first2);
        System.out.println("Second int: " + second2);
    }
}
