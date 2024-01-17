package intellistream.morphstream.api.output;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.common.io.Rdma.Memory.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ResultBatch {
    private final int receiverThreads;
    private final int sendThreads;
    private final int totalSize;
    private final ConcurrentHashMap<Integer, List<FunctionMessage>> results = new ConcurrentHashMap<>();//threadId -> List<FunctionMessage>
    private final ConcurrentHashMap<Integer, Integer> encoded_Lengths = new ConcurrentHashMap<>();//threadId -> encoded_length
    private final ConcurrentHashMap<Integer, Integer> functionExecutorTotalResultCountMap = new ConcurrentHashMap<>();//functionExecutorId -> total result count
    // START_FLAG(Short) + TotalLength(Int) +  MessageBlockLength(Int) * totalThreads + msg.length(Int) + msg + msg.length(Int) + msg... + EndFlag(Short)
    public ResultBatch(int totalSize, int receiverThreads, int senderThreads) {
        this.receiverThreads = receiverThreads;
        this.sendThreads = senderThreads;
        this.totalSize = totalSize;
        for (int i = 0; i < senderThreads; i++) {
            results.put(i, new ArrayList<>());
            encoded_Lengths.put(i, 0);
            functionExecutorTotalResultCountMap.put(i, 0);
        }
    }
    public void add(int senderThreadId, FunctionMessage msg) {
        if (msg == null)
            throw new RuntimeException("null object forbidden in message batch");
        results.get(senderThreadId).add(msg);
        encoded_Lengths.put(senderThreadId, encoded_Lengths.get(senderThreadId) + msgEncodeLength(msg));
        functionExecutorTotalResultCountMap.put(senderThreadId, functionExecutorTotalResultCountMap.get(senderThreadId) + 1);
    }
    private int msgEncodeLength(FunctionMessage workerMessage) {
        if (workerMessage == null) return 0;

        int size = 4; //INT
        if (workerMessage.message() != null)
            size += workerMessage.getEncodeLength();
        return size;
    }
    public ByteBuffer buffer() {
        int totalEncodedLength = 0;
        List<FunctionMessage> totalMessage = new ArrayList<>();
        for (int i = 0; i < sendThreads; i++) {
            totalEncodedLength += encoded_Lengths.get(i);
        }
        for (int i = 0; i < sendThreads; i++) {
            totalMessage.addAll(results.get(i));
        }
        //START_FLAG(Short) + TotalLength(Int) + MessageBlockLength(Int) * totalThreads + EndFlag(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(2 + 4 + this.receiverThreads * 4 + totalEncodedLength + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);//START_FLAG(Short)
            bout.writeInt(totalEncodedLength);//TotalLength(Int)
            Deque<Integer> length = new ArrayDeque<>();
            for (int i = 0; i < receiverThreads; i++) {
                int totalLength = 0;
                for (int j = i * totalSize / receiverThreads; j < (i + 1) * totalSize / receiverThreads; j ++) {
                    totalLength += totalMessage.get(j).getEncodeLength() + 4;
                }
                length.add(totalLength);
            }
            while (!length.isEmpty()) { //MessageBlockLength(Int) * totalThreads
                bout.writeInt(length.poll());
            }
            for (FunctionMessage msg : totalMessage) {//msg.length(Int) + msg + msg.length(Int) + msg...
                bout.writeInt(msg.getEncodeLength());
                bout.write(msg.message());
            }
            bout.writeShort(SOURCE_CONTROL.END_FLAG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return bout.buffer();
    }
    public int getTotalResultCount(int functionExecutorId) {
        return functionExecutorTotalResultCountMap.get(functionExecutorId);
    }
    public void clear() {
        for (int i = 0; i < sendThreads; i++) {
            results.get(i).clear();
            encoded_Lengths.put(i, 0);
            functionExecutorTotalResultCountMap.put(i, 0);
        }
    }
}
