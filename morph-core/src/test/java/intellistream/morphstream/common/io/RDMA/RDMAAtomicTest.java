package intellistream.morphstream.common.io.RDMA;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class RDMAAtomicTest extends TestCase {
    public RDMAAtomicTest(String testName) {
        super(testName);
    }

    public void testCAS() {
        assertTrue(true);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        long bid = 1480;
        long swapValue = bid << 32;
        System.out.println("Swap value: " + swapValue);
        byteBuffer.putLong(swapValue);
        byteBuffer.flip();
        int first = byteBuffer.getInt();
        int second = byteBuffer.getInt();
        System.out.println("First short: " + first);
        System.out.println("Second short: " + second);
    }
    public void testFAA() {
        assertTrue(true);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        long bid = 1480;
        long swapValue = bid << 32;
        System.out.println("Swap value: " + swapValue);
        byteBuffer.putLong(swapValue);
        byteBuffer.flip();
        int first = byteBuffer.getInt();
        int second = byteBuffer.getInt();
        System.out.println("First int: " + first);
        System.out.println("Second int: " + second);
        byteBuffer.flip();
        long before = byteBuffer.getLong();
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(8);
        byteBuffer1.putLong(before + 1);
        byteBuffer1.flip();
        int first1 = byteBuffer1.getInt();
        int second1 = byteBuffer1.getInt();
        System.out.println("First int: " + first1);
        System.out.println("Second int: " + second1);
    }
    public void testFAAD() {
        assertTrue(true);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        long bid = 1480;
        long swapValue = bid << 32;
        System.out.println("Swap value: " + swapValue);
        byteBuffer.putLong(swapValue);
        byteBuffer.flip();
        int first = byteBuffer.getInt();
        int second = byteBuffer.getInt();
        System.out.println("First int: " + first);
        System.out.println("Second int: " + second);
        byteBuffer.flip();
        long before = byteBuffer.getLong();
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(8);
        byteBuffer1.putLong(before + 1);
        byteBuffer1.flip();
        int first1 = byteBuffer1.getInt();
        int second1 = byteBuffer1.getInt();
        System.out.println("First int: " + first1);
        System.out.println("Second int: " + second1);
        byteBuffer1.flip();
        long before1 = byteBuffer1.getLong();
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(8);
        byteBuffer2.putLong(before1 + (-1L));
        byteBuffer2.flip();
        int first2 = byteBuffer2.getInt();
        int second2 = byteBuffer2.getInt();
        System.out.println("First int: " + first2);
        System.out.println("Second int: " + second2);
    }
}
