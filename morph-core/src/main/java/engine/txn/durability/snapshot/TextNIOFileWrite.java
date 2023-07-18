package engine.txn.durability.snapshot;

import common.io.ByteIO.DataInputView;
import common.io.ByteIO.DataOutputView;
import common.io.ByteIO.InputWithDecompression.XORDataInputView;
import common.io.ByteIO.OutputWithCompression.XORDataOutputView;
import util.tools.Deserialize;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;

public class TextNIOFileWrite {
    public static void main(String[] args) throws Exception {
       readNioFuture();
    }
    public static void writeNio() throws IOException, InterruptedException {
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/text.txt");
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, WRITE, CREATE);
        WriteHandler handler = new WriteHandler();
        ByteBuffer dataBuffer = getDataBuffer();
        Attachment attach = new Attachment();
        attach.asyncChannel = afc;
        attach.buffer = dataBuffer;
        attach.path = path;
        afc.write(dataBuffer, 0, attach, handler);
        Thread.sleep(5000);
    }
    public static void readNio() throws IOException, InterruptedException {
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/snapshot/text.txt");
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ,
                CREATE);
        ReadHandler handler = new ReadHandler();
        ByteBuffer dataBuffer = ByteBuffer.allocate((int) afc.size());
        Attachment attach = new Attachment();
        attach.asyncChannel = afc;
        attach.buffer = dataBuffer;
        attach.path = path;
        afc.read(dataBuffer, 0, attach, handler);
        System.out.println("Sleeping for 5  seconds...");
        Thread.sleep(5000);
    }
    public static void readNioFuture() throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/Benchmark/text.txt");
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ);
        int fileSize = (int) afc.size();
        ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
        Future<Integer> result = afc.read(dataBuffer, 0);
        int readBytes = result.get();
        DataInputView inputView = new XORDataInputView(dataBuffer);
        byte[] object = inputView.readFullyDecompression();
        Object t = Deserialize.Deserialize(object);
        Test tt = (Test) t;
        System.out.println(tt.b);
        boolean b = inputView.readBoolean();
        long l = inputView.readLong();
        double d = inputView.readDouble();
        System.out.println(b);
        System.out.println(l);
        System.out.println(d);
    }
    public static void read() throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {
        Path path = Paths.get("/Users/curryzjj/hair-loss/SC/MorphStreamDR/snapshot/text.txt");
        FileInputStream inputStream = new FileInputStream("/Users/curryzjj/hair-loss/SC/MorphStreamDR/snapshot/text.txt");
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        int length = dataInputStream.readInt();
        System.out.println(length);
    }
    static class Attachment {
        public Path path;
        public ByteBuffer buffer;
        public AsynchronousFileChannel asyncChannel;
    }
    static class Test implements Serializable {
        public int a = 0;
        public int b = 1;
    }
    public static ByteBuffer getDataBuffer() throws IOException {
        byte[] object;
        Test test = new Test();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(test);
        oos.flush();
        object = baos.toByteArray();
        DataOutputView dataOutputView = new XORDataOutputView();
        dataOutputView.writeCompression(object);
        dataOutputView.writeBoolean(true);
        dataOutputView.writeLong(90L);
        dataOutputView.writeDouble(0.9);
        ByteBuffer bb = ByteBuffer.wrap(dataOutputView.getByteArray());
        return bb;
    }
    static class WriteHandler implements CompletionHandler<Integer, Attachment> {
        @Override
        public void completed(Integer result, Attachment attach) {
            System.out.format("%s bytes written  to  %s%n", result,
                    attach.path.toAbsolutePath());
            try {
                attach.asyncChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable exc, Attachment attach) {
            try {
                attach.asyncChannel.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    static class ReadHandler implements CompletionHandler<Integer, Attachment> {
        @Override
        public void completed(Integer result, Attachment attach) {
            System.out.format("%s bytes read   from  %s%n", result, attach.path);
            System.out.format("Read data is:%n");
            byte[] byteData = attach.buffer.array();
            Charset cs = Charset.forName("UTF-8");
            String data = new String(byteData, cs);
            System.out.println(data);
            try {
                // Close the channel
                attach.asyncChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable e, Attachment attach) {
            System.out.format("Read operation  on  %s  file failed." + "The  error is: %s%n", attach.path, e.getMessage());
            try {
                // Close the channel
                attach.asyncChannel.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}