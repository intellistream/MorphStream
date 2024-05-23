package intellistream.morphstream.util;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Random;

public class FixedLengthRandomString {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new SecureRandom();

    public static void main(String[] args) {
        int fixedLength = 64;

        try {
            String fixedLengthString = generateRandomFixedLengthString(fixedLength);
            System.out.println(fixedLengthString);
            System.out.println("Length in bytes: " + fixedLengthString.getBytes(StandardCharsets.UTF_8).length);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateRandomFixedLengthString(int length){
        byte[] fixedLengthBytes = new byte[length];

        // 使用随机字符填充固定长度的字节数组
        int byteCount = 0;
        while (byteCount < length) {
            char randomChar = CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length()));
            byte[] charBytes = String.valueOf(randomChar).getBytes(StandardCharsets.UTF_8);

            if (byteCount + charBytes.length > length) {
                break; // 确保不会超过固定字节长度
            }

            System.arraycopy(charBytes, 0, fixedLengthBytes, byteCount, charBytes.length);
            byteCount += charBytes.length;
        }

        // 如果不足，则用空格填充
        for (int i = byteCount; i < length; i++) {
            fixedLengthBytes[i] = ' ';
        }

        return new String(fixedLengthBytes, StandardCharsets.UTF_8);
    }
}