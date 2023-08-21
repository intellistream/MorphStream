package common.tools;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Deserialize {
    public static <T> T Deserialize2Object(byte[] value, ClassLoader cl) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(value);
        ObjectInputStream oois = new ObjectInputStream(bais);
        Thread.currentThread().setContextClassLoader(cl);
        return (T) oois.readObject();
    }
    public static Object Deserialize(byte[] bytes) {
        ByteArrayInputStream byteArrayInputStream = null;
        ObjectInputStream inputStream = null;
        try{
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            inputStream = new ObjectInputStream(byteArrayInputStream);
            return inputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
