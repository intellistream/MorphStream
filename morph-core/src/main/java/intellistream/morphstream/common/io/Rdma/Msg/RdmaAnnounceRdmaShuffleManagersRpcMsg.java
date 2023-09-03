package intellistream.morphstream.common.io.Rdma.Msg;

import intellistream.morphstream.common.io.Rdma.RdmaUtils.Id.RdmaShuffleManagerId;
import javafx.util.Pair;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RdmaAnnounceRdmaShuffleManagersRpcMsg implements RdmaRpcMsg{
    public List<RdmaShuffleManagerId> rdmaShuffleManagerIds;
    public RdmaAnnounceRdmaShuffleManagersRpcMsg(List<RdmaShuffleManagerId> rdmaShuffleManagerIds) {
        this.rdmaShuffleManagerIds = rdmaShuffleManagerIds;
    }
    private RdmaAnnounceRdmaShuffleManagersRpcMsg() {
        // For deserialization only
    }

    @Override
    public RdmaRpcMsgType msgType() {
        return RdmaRpcMsgType.AnnounceRdmaShuffleManagers;
    }

    @Override
    public int[] getLengthInSegments(int segmentSize) {//每个segment累加序列化长度
        List<Integer> segmentSizes = new ArrayList<>();
        int curSegmentLength = 0;
        for (RdmaShuffleManagerId rdmaShuffleManagerId : rdmaShuffleManagerIds) {
            int serializedLength = rdmaShuffleManagerId.serializedLength();
            if (!segmentSizes.isEmpty() && curSegmentLength + serializedLength <= segmentSize) {
                curSegmentLength += serializedLength;
                segmentSizes.add(curSegmentLength);
            } else {
                curSegmentLength = serializedLength;
                segmentSizes.add(serializedLength);
            }
        }
        int[] sizesArray = new int[segmentSizes.size()];
        for (int i = 0; i < segmentSizes.size(); i++) {
            sizesArray[i] = segmentSizes.get(i);
        }

        return sizesArray;
    }

    @Override
    public void writeSegments(Iterator<Pair<DataOutputStream, Integer>> outs) throws IOException {
        DataOutputStream curOut = null;
        int curSegmentLength = 0;

        while (outs.hasNext()) {
            Pair<DataOutputStream, Integer> pair = outs.next();
            curOut = pair.getKey();
            int segmentSize = pair.getValue();

            for (RdmaShuffleManagerId rdmaShuffleManagerId : rdmaShuffleManagerIds) {
                int serializedLength = rdmaShuffleManagerId.serializedLength();

                if (curSegmentLength + serializedLength > segmentSize) {
                    // If adding this serializedLength exceeds the segment size, switch to the next output stream
                    curSegmentLength = 0;
                }

                curSegmentLength += serializedLength;
                rdmaShuffleManagerId.write(curOut);
            }
        }
    }

    @Override
    public void read(DataInputStream in) throws IOException {
        ArrayList<RdmaShuffleManagerId> tmpRdmaShuffleManagerIds = new ArrayList<>();

        try {
            while (true) {
                RdmaShuffleManagerId rdmaShuffleManagerId = RdmaShuffleManagerId.apply(in);
                tmpRdmaShuffleManagerIds.add(rdmaShuffleManagerId);
            }
        } catch (EOFException e) {
            // End of input stream, stop reading
        }

        rdmaShuffleManagerIds = tmpRdmaShuffleManagerIds;
    }

    public static RdmaAnnounceRdmaShuffleManagersRpcMsg apply(DataInputStream in) throws IOException {
        RdmaAnnounceRdmaShuffleManagersRpcMsg obj = new RdmaAnnounceRdmaShuffleManagersRpcMsg();
        obj.read(in);
        return obj;
    }
}
