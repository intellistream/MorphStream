package common.tools;

import java.io.*;

public class Serialize {
    public static byte[] serializeObject(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }
    public static void writeSerializeObject(OutputStream out, Object o) throws IOException{
        ObjectOutputStream oos =
                out instanceof ObjectOutputStream
                        ? (ObjectOutputStream) out
                        : new ObjectOutputStream(out);
        oos.writeObject(o);
    }

    public static Object cloneObject(Object o) throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
    }
}
