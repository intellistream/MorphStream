package common.collections;

import sun.misc.Unsafe;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;

//import static xerial.jnuma.Numa.numNodes;

/**
 * Created by I309939 on 5/3/2016.
 */
public final class OsUtils {
    private static final String OS = System.getProperty("os.name").toLowerCase();

    public static long getJVMID() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        String jvmName = runtimeBean.getName();
        return Long.valueOf(jvmName.split("@")[0]);
    }

    public static long getPID() {
        return getJVMID();
    }

    public static void detectOS() {
        if (isWindows()) {
        } else if (isMac()) {
        } else if (isUnix()) {
        } else {
        }
    }

    public static String OS_wrapper(String path) {
        if (isWindows()) {
            return "\\" + path;
        } else if (isMac()) {
            return "/" + path;
        } else if (isUnix()) {
            return "/" + path;
        } else {
            return null;
        }
    }

    public static String osWrapperPostFix(String path) {
        if (isWindows()) {
            return path + "\\";
        } else if (isMac()) {
            return path + "/";
        } else if (isUnix()) {
            return path + "/";
        } else {
            return null;
        }
    }

    public static boolean isWindows() {
        return (OS.contains("win"));
    }

    public static boolean isMac() {
        return (OS.contains("mac"));
    }

    public static boolean isUnix() {
        return (OS.contains("nux"));
    }

    public static int TotalCores() {
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    //    public static int totalSockets() {
//        return numNodes();
//    }
    public static class Addresser {
        private static Unsafe unsafe;

        static {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (Unsafe) field.get(null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static long addressOf(Object o) {
            Object[] array = new Object[]{o};
            long baseOffset = unsafe.arrayBaseOffset(Object[].class);
            int addressSize = unsafe.addressSize();
            long objectAddress;
            switch (addressSize) {
                case 4:
                    objectAddress = unsafe.getInt(array, baseOffset);
                    break;
                case 8:
                    objectAddress = unsafe.getLong(array, baseOffset);
                    break;
                default:
                    throw new Error("unsupported address fieldSize: " + addressSize);
            }
            return (objectAddress);
        }

        public static void main(String... args) {
            Object mine = "Hi there".toCharArray();
            long address = addressOf(mine);
            System.out.println("Address: " + address);
            //Verify address works - should see the characters in the array in the output
            //  printBytes(address, 27); -- causes JVM crash sometimes.
        }

        public static void printBytes(long objectAddress, int num) {
            for (long i = 0; i < num; i++) {
                int cur = unsafe.getByte(objectAddress + i);
                System.out.print((char) cur);
            }
            System.out.println();
        }
    }
}