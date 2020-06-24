package application.tasks;

import org.apache.commons.lang.RandomStringUtils;
import sun.misc.Unsafe;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * Created by shuhaozhang on 23/8/16.
 */
public abstract class stateless_task implements Serializable {
    private static final long serialVersionUID = 9L;

    private static Unsafe getUnsafe() throws Exception {
        // Get the Unsafe object instance
        Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        field.setAccessible(true);
        return (sun.misc.Unsafe) field.get(null);
    }

    public static void showBytes() {
        try {
            Unsafe unsafe = getUnsafe();

            // Writing to a memory - MAX VALUE Byte
            byte value = Byte.MAX_VALUE;
            long bytes = 1;
            // Allocate given memory size
            long memoryAddress = unsafe.allocateMemory(bytes);
            // Write value to the allocated memory
            unsafe.putAddress(memoryAddress, value); // or putByte

            // Output the value written and the memory address
            //System.out.println("[Byte] Writing " + value + " under the " + memoryAddress + " address.");

            long readValue = unsafe.getAddress(memoryAddress); // or getByte

            // Output the value from
            //System.out.println("[Byte] Reading " + readValue + " from the " + memoryAddress + " address.");

            // C style! Release the Kraken... Memory!! :)
            unsafe.freeMemory(memoryAddress);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static long showLong() {
        try {
            Unsafe unsafe = getUnsafe();

            // Writing to a memory - MAX VALUE of Long
            long value = 1l;
            long bytes = Long.SIZE;
            // Allocate given memory size
            long memoryAddress = unsafe.allocateMemory(bytes);
            // Write value to the allocated memory
            unsafe.putLong(memoryAddress, value);

            // Output the value written and the memory address
            //System.out.println("[Long] Writing " + value + " under the " + memoryAddress + " address.");

            // Read the value from the memory
            long readValue = unsafe.getLong(memoryAddress);


            // Output the value from
            //System.out.println("[Long] Reading " + readValue + " from the " + memoryAddress + " address.");

            // C style! Release the Kraken... Memory!! :)
            unsafe.freeMemory(memoryAddress);
            return readValue;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int random_compute(int size) {
        String generatedString = RandomStringUtils.randomAlphanumeric(size);
        int i = 0;
        for (char c : generatedString.toCharArray())
            i += c;
        return i;
    }

    public static int random_compute(String value) {
        int i = 0;
        for (char c : value.toCharArray())
            i += c;
        return i;
    }

    /**
     * used in normal call
     *
     * @param value
     * @return
     */
    public abstract int execute(String value);

    /**
     * used in verbose call
     *
     * @param value
     * @param function_process_start
     * @return
     */
    public abstract String execute(String value, long function_process_start);
}
