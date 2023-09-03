package intellistream.morphstream.common.io.Rdma.Msg;

import intellistream.morphstream.common.io.Rdma.RdmaUtils.Id.RdmaShuffleManagerId;
import javafx.util.Pair;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class RdmaShuffleManagerHelloRpcMsg implements RdmaRpcMsg {
    public RdmaShuffleManagerId rdmaShuffleManagerId;
    public int channelPort;
    public RdmaShuffleManagerHelloRpcMsg(RdmaShuffleManagerId rdmaShuffleManagerId, int channelPort) {
        this.rdmaShuffleManagerId = rdmaShuffleManagerId;
        this.channelPort = channelPort;
    }
    // For deserialization only
    private RdmaShuffleManagerHelloRpcMsg() {
        this(null, 0);
    }
    @Override
    public RdmaRpcMsgType msgType() {
        return RdmaRpcMsgType.RdmaShuffleManagerHello;
    }
    @Override
    public int[] getLengthInSegments(int segmentSize) {
        int serializedLength = rdmaShuffleManagerId.serializedLength() + 4;
        if (serializedLength > segmentSize) {
            throw new IllegalArgumentException("RdmaBuffer RPC segment size is too small");
        }

        return new int[]{serializedLength};
    }
    @Override
    public void writeSegments(Iterator<Pair<DataOutputStream, Integer>> outs) throws IOException {
        Pair<DataOutputStream, Integer> outPair = outs.next();
        DataOutputStream out = outPair.getKey();
        rdmaShuffleManagerId.write(out);
        out.writeInt(channelPort);
    }

    @Override
    public void read(DataInputStream in) throws IOException {
        rdmaShuffleManagerId = RdmaShuffleManagerId.apply(in);
        channelPort = in.readInt();
    }

    public static RdmaRpcMsg apply(DataInputStream in) throws IOException {
        RdmaShuffleManagerHelloRpcMsg obj = new RdmaShuffleManagerHelloRpcMsg();
        obj.read(in);
        return obj;
    }
}
