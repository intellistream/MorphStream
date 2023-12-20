package cli;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ByteTest {

//    public static void main(String[] args) throws Exception {
//
//        byte[] byteArray = {-112, -32, -1, 71, 0, 0, 0, 0, 59, -112, -32, -1, 71, 0, 0, 0, 0, 59, 48, 48, 48, 48, 58, 48, 48, 48, 49, 59, 0, 0, 0, 0, 59, 0};
//
//        // Step 1: Split the byte array by number 59
//        String[] subByteArrays = splitFullByteArray(byteArray, (byte) 59);
//
//        String bid = subByteArrays[0];
//        String reqID = subByteArrays[1];
//        String keys = subByteArrays[2];
//        String flag = subByteArrays[3];
//        String isAbort = subByteArrays[4];
//
//        byte[] bidBytes = bid.getBytes();
//
//        // Step 2: Convert these five sub byte arrays into long, long, String, int, boolean separately
//        long firstLong = bytesToLong(subByteArrays[0].getBytes());
//        long secondLong = bytesToLong(subByteArrays[1].getBytes());
//
//        int splitIndex = 4; // Define the index to split the array
//        byte[] array1 = new byte[splitIndex];
//        byte[] array2 = new byte[byteArray.length - splitIndex];
//        // Copy elements to the first array
//        System.arraycopy(byteArray, 0, array1, 0, splitIndex);
//        // Copy elements to the second array
//        System.arraycopy(byteArray, splitIndex + 1, array2, 0, byteArray.length - splitIndex - 1);
//
//        int intValue = Integer.parseInt(subByteArrays[3]);
//        boolean boolValue = subByteArrays[4].getBytes()[0] != 0;
//
//
////        insertInputData(decodedString);
//    }

    public static void main(String[] args) {
        byte[] byteArray = {-112, -32, -1, 71, 0, 0, 0, 0, 59, -112, -32, -1, 71, 0, 0, 0, 0, 59, 48, 48, 48, 48, 58, 48, 48, 48, 49, 59, 0, 0, 0, 0, 59, 0, 0, 0, 0};

        byte fullSeparator = 59;
        byte keySeparator = 58;
        List<byte[]> splitByteArrays = splitByteArray(byteArray, fullSeparator);

        byte[] bidByte = splitByteArrays.get(0);
        byte[] reqIDByte = splitByteArrays.get(1);
        byte[] keysByte = splitByteArrays.get(2);
        byte[] flagByte = splitByteArrays.get(3);
        byte[] isAbortByte = splitByteArrays.get(4);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.clear();
        buffer.put(bidByte);
        buffer.flip();
        long bid = buffer.getLong();

        buffer.clear();
        buffer.put(reqIDByte);
        buffer.flip();
        long reqID = buffer.getLong();

        List<byte[]> splitKeyByteArrays = splitByteArray(keysByte, keySeparator);
        for (byte[] i : splitKeyByteArrays) {
            String str = new String(i, StandardCharsets.US_ASCII);
            System.out.println(str);
        }

        int flag = ByteBuffer.wrap(flagByte).getInt();
        System.out.println(flag);

        int isAbort = ByteBuffer.wrap(flagByte).getInt();
        System.out.println(isAbort);

    }

    private static List<byte[]> splitByteArray(byte[] byteArray, byte separator) {
        List<byte[]> splitByteArrays = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < byteArray.length; i++) {
            if (byteArray[i] == separator) {
                indexes.add(i);
            }
        }

        int startIndex = 0;
        for (Integer index : indexes) {
            byte[] subArray = new byte[index - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, index - startIndex);
            splitByteArrays.add(subArray);
            startIndex = index + 1;
        }

        // Handling the remaining part after the last occurrence of 59
        if (startIndex < byteArray.length) {
            byte[] subArray = new byte[byteArray.length - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, byteArray.length - startIndex);
            splitByteArrays.add(subArray);
        }

        return splitByteArrays;
    }

}
