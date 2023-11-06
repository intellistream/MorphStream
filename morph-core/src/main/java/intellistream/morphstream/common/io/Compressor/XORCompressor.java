package intellistream.morphstream.common.io.Compressor;

/**
 * 异或加密：
 * 某个字符或者数值 x 与一个数值 m 进行异或运算得到 y ,
 * 则再用 y 与 m 进行异或运算就可还原为 x
 * 使用场景：
 * 1. 两个变量的互换（不借助第三个变量）
 * 2. 数据的简单加密解密
 */
public class XORCompressor implements Compressor {
    private final int key = 0x12;

    /**
     * 固定key方式加解密
     *
     * @param data 原字符串
     * @param key
     * @return
     */
    public static String encrypt(String data, int key) {
        byte[] in = data.getBytes();
        int length = in.length;
        for (int i = 0; i < length; i++) {
            in[i] = (byte) (in[i] ^ key);
            key = in[i];
        }
        return new String(in);
    }

    public static String decrypt(String data, int key) {
        byte[] in = data.getBytes();
        int len = in.length;
        for (int i = len - 1; i > 0; i--) {
            in[i] = (byte) (in[i] ^ in[i - 1]);
        }
        in[0] = (byte) (in[0] ^ key);
        return new String(in);
    }

    @Override
    public String compress(String in) {
        return encrypt(in, key);
    }

    @Override
    public String uncompress(String in) {
        return decrypt(in, key);
    }
}

