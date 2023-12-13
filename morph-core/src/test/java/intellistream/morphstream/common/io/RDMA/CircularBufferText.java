package intellistream.morphstream.common.io.RDMA;


import intellistream.morphstream.common.io.Unsafe.Platform;
import intellistream.morphstream.common.io.Unsafe.memory.MemoryBlock;
import intellistream.morphstream.common.io.Unsafe.memory.UnsafeMemoryAllocator;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static intellistream.morphstream.common.io.Rdma.Memory.RdmaBuffer.directBufferConstructor;

public class CircularBufferText extends TestCase {
    public CircularBufferText(String testName) {
        super(testName);
    }
    public static Test suite()
    {
        return new TestSuite( CircularBufferText.class );
    }
    public static final Constructor<?> directBufferConstructor;
    static {
        try {
            Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
            directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
            directBufferConstructor.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException("java.nio.DirectByteBuffer class not found");
        }
    }
    public void testApp() throws IOException {
        UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();
        MemoryBlock memoryBlock = unsafeAlloc.allocate(1024 * 1024);
        ByteBuffer writebyteBuffer = null;
        try {
            writebyteBuffer = (ByteBuffer) directBufferConstructor.newInstance(memoryBlock.getBaseOffset(), 1024 * 1024);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        String[] strings = "string1, string2, string3, string4, string5, string6, string7, string8, string9, string10".split(",");
        List<String> strings1 = new ArrayList<>();
        for (String string : strings) {
            strings1.add(string.trim());
        }
        Deque<Integer> length = new ArrayDeque<>();
        Deque<byte[]> bytes = new ArrayDeque<>();
        int totalThread = 2;
        for (int i = 0; i < totalThread; i++) {
            int totalLength = 0;
            for (int j = i * strings1.size() / totalThread; j < (i + 1) * strings1.size() / totalThread; j++) {
                byte[] bytes1 = strings1.get(j).getBytes(StandardCharsets.UTF_8);
                bytes.add(bytes1);
                totalLength += strings1.get(j).getBytes(StandardCharsets.UTF_8).length + 4;
            }
            length.add(totalLength);
        }
        while (length.size() > 0) {
            writebyteBuffer.putInt(length.poll());
        }
        for (int i = 0; i < strings1.size(); i++) {
            byte[] bytes1 = bytes.poll();
            writebyteBuffer.putInt(bytes1.length);
            writebyteBuffer.put(bytes1);
        }

        for (int i = 0; i < 2; i++) {
            ReaderThread readerThread = new ReaderThread(i, memoryBlock, 2);
            Thread thread = new Thread(readerThread);
            thread.start();
        }
    }

    static class ReaderThread implements Runnable {
        public int index;
        public int totalThread;
        public MemoryBlock memoryBlock;
        public ByteBuffer byteBuffer;
        public long offset;
        public List<Integer> lengthQueue = new ArrayList<>();
        public ReaderThread(int index, MemoryBlock memoryBlock, int totalThread) {
            this.index = index;
            this.memoryBlock = memoryBlock;
            this.totalThread = totalThread;
            this.offset = memoryBlock.getBaseOffset() + totalThread * 4;
            try {
                ByteBuffer offset = (ByteBuffer) directBufferConstructor.newInstance(memoryBlock.getBaseOffset(), totalThread * 4);
                while (offset.hasRemaining()) {
                    lengthQueue.add(offset.getInt());
                }
                for (int i = 0; i < index; i++) {
                    this.offset += lengthQueue.get(i);
                }
                this.byteBuffer = (ByteBuffer) directBufferConstructor.newInstance(this.offset, lengthQueue.get(index));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            while (byteBuffer.hasRemaining()) {
                int length1 = byteBuffer.getInt();
                byte[] bytes1 = new byte[length1];
                byteBuffer.get(bytes1);
                System.out.println(this.index + ": " + new String(bytes1, StandardCharsets.UTF_8));
            }
        }
    }
    public static <T> List<List<T>> partitionList(List<T> inputList, int partitionSize) {
        if (inputList == null || inputList.isEmpty() || partitionSize <= 0) {
            throw new IllegalArgumentException("Invalid input");
        }

        int totalSize = inputList.size();
        int numOfPartitions = (int) Math.ceil((double) totalSize / partitionSize);

        List<List<T>> partitions = new ArrayList<>(numOfPartitions);

        for (int i = 0; i < totalSize; i += partitionSize) {
            int end = Math.min(i + partitionSize, totalSize);
            partitions.add(new ArrayList<>(inputList.subList(i, end)));
        }

        return partitions;
    }
}
