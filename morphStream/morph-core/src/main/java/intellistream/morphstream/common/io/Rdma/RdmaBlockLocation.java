package intellistream.morphstream.common.io.Rdma;

import java.io.*;
import java.nio.charset.Charset;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.apache.spark.ShuffleDependency;
//import org.apache.spark.shuffle.BaseShuffleHandle;
//import org.apache.spark.shuffle.sort.SerializedShuffleHandle;
//import org.apache.spark.storage.BlockManagerId;

public class RdmaBlockLocation {
    public long address;
    public int length;
    public int mKey;

    public RdmaBlockLocation(long address, int length, int mKey) {
        this.address = address;
        this.length = length;
        this.mKey = mKey;
        assert length >= 0 : "Block length must be >=0, but " + length + " is set";
    }
}

//class SerializableBlockManagerId {
//    private final BlockManagerId blockManagerId;
//    private byte[] executorIdInUtf;
//    private byte[] blockManagerHostNameInUtf;
//
//    public SerializableBlockManagerId(String executorId, String host, int port) {
//        blockManagerId = new BlockManagerId(executorId, host, port);
//    }
//
//    public BlockManagerId toBlockManagerId() {
//        return blockManagerId;
//    }
//
//    public int serializedLength() {
//        if (executorIdInUtf == null) {
//            executorIdInUtf = blockManagerId.executorId().getBytes(Charset.forName("UTF-8"));
//        }
//        if (blockManagerHostNameInUtf == null) {
//            blockManagerHostNameInUtf = blockManagerId.host().getBytes(Charset.forName("UTF-8"));
//        }
//        return 2 + executorIdInUtf.length + 2 + blockManagerHostNameInUtf.length + 4;
//    }
//
//    public void write(DataOutputStream out) throws IOException {
//        out.writeShort(executorIdInUtf.length);
//        out.write(executorIdInUtf);
//        out.writeShort(blockManagerHostNameInUtf.length);
//        out.write(blockManagerHostNameInUtf);
//        out.writeInt(blockManagerId.port());
//    }
//
//    public static SerializableBlockManagerId apply(DataInputStream in) throws IOException {
//        short executorIdLen = in.readShort();
//        byte[] executorIdInUtf = new byte[executorIdLen];
//        in.readFully(executorIdInUtf);
//        String executorId = new String(executorIdInUtf, "UTF-8");
//
//        short blockManagerHostNameLen = in.readShort();
//        byte[] blockManagerHostNameInUtf = new byte[blockManagerHostNameLen];
//        in.readFully(blockManagerHostNameInUtf);
//        String blockManagerHostName = new String(blockManagerHostNameInUtf, "UTF-8");
//
//        int port = in.readInt();
//
//        return new SerializableBlockManagerId(executorId, blockManagerHostName, port);
//    }
//}
//
//class RdmaShuffleManagerId {
//    private String host;
//    private int port;
//    private BlockManagerId blockManagerId;
//    private byte[] hostnameInUtf;
//    private SerializableBlockManagerId serializableBlockManagerId;
//
//    public RdmaShuffleManagerId(String host, int port, BlockManagerId blockManagerId) {
//        this.host = host;
//        this.port = port;
//        this.blockManagerId = blockManagerId;
//        if (blockManagerId != null) {
//            serializableBlockManagerId = new SerializableBlockManagerId(blockManagerId);
//        }
//    }
//
//    @Override
//    public String toString() {
//        return "RdmaShuffleManagerId(" + host + ", " + port + ", " + blockManagerId + ")";
//    }
//
//    public int serializedLength() {
//        if (hostnameInUtf == null) {
//            hostnameInUtf = host.getBytes(Charset.forName("UTF-8"));
//        }
//        return 2 + hostnameInUtf.length + 4 + serializableBlockManagerId.serializedLength();
//    }
//
//    public void write(DataOutputStream out) throws IOException {
//        if (hostnameInUtf == null) {
//            hostnameInUtf = host.getBytes(Charset.forName("UTF-8"));
//        }
//        out.writeShort(hostnameInUtf.length);
//        out.write(hostnameInUtf);
//        out.writeInt(port);
//        serializableBlockManagerId.write(out);
//    }
//
//    private void read(DataInputStream in) throws IOException {
//        short hostnameLen = in.readShort();
//        byte[] hostnameInUtf = new byte[hostnameLen];
//        in.readFully(hostnameInUtf);
//        host = new String(hostnameInUtf, "UTF-8");
//        port = in.readInt();
//        serializableBlockManagerId = SerializableBlockManagerId.apply(in);
//        blockManagerId = serializableBlockManagerId.toBlockManagerId();
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj) return true;
//        if (!(obj instanceof RdmaShuffleManagerId)) return false;
//        RdmaShuffleManagerId id = (RdmaShuffleManagerId) obj;
//        return port == id.port && host.equals(id.host) && blockManagerId.equals(id.blockManagerId);
//    }
//
//
//
//    @Override
//    public int hashCode() {
//        return blockManagerId.hashCode();
//    }
//}
//
//class RdmaBaseShuffleHandle<K, V, C> extends BaseShuffleHandle<K, V, C> {
//    final long driverTableAddress;
//    final int driverTableLength;
//    final int driverTableRKey;
//
//    public RdmaBaseShuffleHandle(
//            long driverTableAddress,
//            int driverTableLength,
//            int driverTableRKey,
//            int shuffleId,
//            int numMaps,
//            ShuffleDependency<K, V, C> dependency) {
//        super(shuffleId, numMaps, dependency);
//        this.driverTableAddress = driverTableAddress;
//        this.driverTableLength = driverTableLength;
//        this.driverTableRKey = driverTableRKey;
//    }
//}
//
//class RdmaSerializedShuffleHandle<K, V> extends SerializedShuffleHandle<K, V> {
//    final long driverTableAddress;
//    final int driverTableLength;
//    final int driverTableRKey;
//
//    public RdmaSerializedShuffleHandle(
//            long driverTableAddress,
//            int driverTableLength,
//            int driverTableRKey,
//            int shuffleId,
//            int numMaps,
//            ShuffleDependency<K, V, V> dependency) {
//        super(shuffleId, numMaps, dependency);
//        this.driverTableAddress = driverTableAddress;
//        this.driverTableLength = driverTableLength;
//        this.driverTableRKey = driverTableRKey;
//    }