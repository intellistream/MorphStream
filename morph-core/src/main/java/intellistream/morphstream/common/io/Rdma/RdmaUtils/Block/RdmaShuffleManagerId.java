package intellistream.morphstream.common.io.Rdma.RdmaUtils.Block;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaShuffleManagerId {
    private String host;
    private int port;
    private BlockManagerId blockManagerId;
    private byte[] hostnameInUtf;
    private SerializableBlockManagerId serializableBlockManagerId;
    public RdmaShuffleManagerId(String host, int port, BlockManagerId blockManagerId) {
        this.host = host;
        this.port = port;
        this.blockManagerId = blockManagerId;
        this.hostnameInUtf = null;
        if (blockManagerId != null) {
            this.serializableBlockManagerId = new SerializableBlockManagerId(blockManagerId);
        } else {
            this.serializableBlockManagerId = null;
        }
    }
    // For deserialization only
    private RdmaShuffleManagerId() {
        this(null, 0, null);
    }
    public int serializedLength() {
        if (hostnameInUtf == null) {
            hostnameInUtf = host.getBytes(Charset.forName("UTF-8"));
        }
        return 2 + hostnameInUtf.length + 4 + serializableBlockManagerId.serializedLength();
    }

    public BlockManagerId getBlockManagerId() {
        return blockManagerId;
    }

    public void write(DataOutputStream out) throws IOException {
        if (hostnameInUtf == null) {
            hostnameInUtf = host.getBytes(Charset.forName("UTF-8"));
        }
        out.writeShort(hostnameInUtf.length);
        out.write(hostnameInUtf);
        out.writeInt(port);
        serializableBlockManagerId.write(out);
    }
    public void read(DataInputStream in) throws IOException {
        hostnameInUtf = new byte[in.readShort()];
        in.readFully(hostnameInUtf);
        host = new String(hostnameInUtf, Charset.forName("UTF-8"));
        port = in.readInt();
        serializableBlockManagerId = SerializableBlockManagerId.apply(in);
        blockManagerId = serializableBlockManagerId.toBlockManagerId();
    }
    public static RdmaShuffleManagerId apply(String host, int port, BlockManagerId blockManagerId) {
        return getCachedRdmaShuffleManagerId(new RdmaShuffleManagerId(host, port, blockManagerId));
    }

    public static RdmaShuffleManagerId apply(DataInputStream in) throws IOException {
        RdmaShuffleManagerId obj = new RdmaShuffleManagerId();
        obj.read(in);
        return getCachedRdmaShuffleManagerId(obj);
    }

    private static final ConcurrentHashMap<RdmaShuffleManagerId, RdmaShuffleManagerId> rdmaShuffleManagerIdIdCache = new ConcurrentHashMap<>();

    private static RdmaShuffleManagerId getCachedRdmaShuffleManagerId(RdmaShuffleManagerId id) {
        rdmaShuffleManagerIdIdCache.putIfAbsent(id, id);
        return rdmaShuffleManagerIdIdCache.get(id);
    }
    @Override
    public String toString() {
        return "RdmaShuffleManagerId(" + host + ", " + port + ", " + blockManagerId + ")";
    }

    @Override
    public int hashCode() {
        return blockManagerId.hashCode();
    }
}
