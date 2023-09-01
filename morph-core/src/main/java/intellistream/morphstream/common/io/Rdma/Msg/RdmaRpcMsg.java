package intellistream.morphstream.common.io.Rdma.Msg;

import intellistream.morphstream.common.io.Rdma.ByteBufferBackedInputStream;
import intellistream.morphstream.common.io.Rdma.RdmaByteBufferManagedBuffer;
import javafx.util.Pair;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

public interface RdmaRpcMsg {
    RdmaRpcMsgType msgType();
    int[] getLengthInSegments(int segmentSize);
    void writeSegments(Iterator<Pair<DataOutputStream, Integer>> outs) throws IOException;
    void read(DataInputStream in) throws IOException;
    default int overhead() {
        return 4 + 4; // 4 + 4 for msg length and type
    }
    default RdmaByteBufferManagedBuffer[] toRdmaByteBufferManagedBuffers(IntFunction<RdmaByteBufferManagedBuffer> allocator, int maxSegmentSize) throws IOException {
        int[] arrSegmentLengths = getLengthInSegments(maxSegmentSize - overhead());
        RdmaByteBufferManagedBuffer[] bufs = new RdmaByteBufferManagedBuffer[arrSegmentLengths.length];

        for (int bufferIndex = 0; bufferIndex < bufs.length; bufferIndex++) {
            bufs[bufferIndex] = allocator.apply(maxSegmentSize);
        }
        List<Pair<DataOutputStream, Integer>> outsList = new ArrayList<>();
        for (int bufferIndex = 0; bufferIndex < bufs.length; bufferIndex++) {
            DataOutputStream out = null;
            try {
                out = new DataOutputStream(bufs[bufferIndex].createOutputStream());
                out.writeInt(overhead() + arrSegmentLengths[bufferIndex]);
                out.writeInt(Integer.parseInt(msgType().name()));
            } catch (IOException e) {
                // 处理异常
            }
            outsList.add(new Pair<>(out, arrSegmentLengths[bufferIndex]));
        }

        Iterator<Pair<DataOutputStream, Integer>> outs = outsList.iterator();
        writeSegments(outs);
        return bufs;
    }
    static RdmaRpcMsg apply(ByteBuffer buf) throws IOException {
        DataInputStream in = new DataInputStream(new ByteBufferBackedInputStream(buf));
        int msgLength = in.readInt();
        buf.limit(msgLength);

        RdmaRpcMsgType msgType = RdmaRpcMsgType.values()[in.readInt()];
        switch (msgType) {
            case RdmaShuffleManagerHello:
                return RdmaShuffleManagerHelloRpcMsg.apply(in);
            case AnnounceRdmaShuffleManagers:
                return RdmaAnnounceRdmaShuffleManagersRpcMsg.apply(in);
            default:
                throw new IllegalArgumentException("Unknown RdmaRpcMsg type: " + msgType);
        }
    }
    enum RdmaRpcMsgType {
        RdmaShuffleManagerHello, AnnounceRdmaShuffleManagers
    }

}


