package intellistream.morphstream.common.io.Rdma.RdmaUtils.Id;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class SerializableBlockManagerId {
    private BlockManagerId blockManagerId;
    private byte[] executorIdInUtf;
    private byte[] blockManagerHostNameInUtf;

    public SerializableBlockManagerId(String executorId, String host, int port) {
        this.blockManagerId = new BlockManagerId(executorId, host, port);
        this.executorIdInUtf = null;
        this.blockManagerHostNameInUtf = null;
    }
    public SerializableBlockManagerId(BlockManagerId blockManagerId) {
        this(blockManagerId.getExecutorId(), blockManagerId.getHost(), blockManagerId.getPort());
    }
    public BlockManagerId toBlockManagerId() {
        return blockManagerId;
    }
    public int serializedLength() {
        if (executorIdInUtf == null) {
            executorIdInUtf = blockManagerId.getExecutorId().getBytes(Charset.forName("UTF-8"));
        }
        if (blockManagerHostNameInUtf == null) {
            blockManagerHostNameInUtf = blockManagerId.getHost().getBytes(Charset.forName("UTF-8"));
        }
        return 2 + executorIdInUtf.length + 2 + blockManagerHostNameInUtf.length + 4;
    }
    public void write(DataOutputStream out) throws IOException {
        if (executorIdInUtf == null) {
            executorIdInUtf = blockManagerId.getExecutorId().getBytes(Charset.forName("UTF-8"));
        }
        if (blockManagerHostNameInUtf == null) {
            blockManagerHostNameInUtf = blockManagerId.getHost().getBytes(Charset.forName("UTF-8"));
        }
        out.writeShort(executorIdInUtf.length);
        out.write(executorIdInUtf);
        out.writeShort(blockManagerHostNameInUtf.length);
        out.write(blockManagerHostNameInUtf);
        out.writeInt(blockManagerId.getPort());
    }

    public static SerializableBlockManagerId apply(DataInputStream in) throws IOException {
        byte[] executorIdInUtf = new byte[in.readShort()];
        in.readFully(executorIdInUtf);
        byte[] blockManagerHostNameInUtf = new byte[in.readShort()];
        in.readFully(blockManagerHostNameInUtf);
        return new SerializableBlockManagerId(
                new String(executorIdInUtf, Charset.forName("UTF-8")),
                new String(blockManagerHostNameInUtf, Charset.forName("UTF-8")),
                in.readInt()
        );
    }
}
