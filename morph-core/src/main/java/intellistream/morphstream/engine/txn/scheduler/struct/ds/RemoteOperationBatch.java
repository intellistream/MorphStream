package intellistream.morphstream.engine.txn.scheduler.struct.ds;

import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RemoteOperationBatch {
    private final int receiverThreads;
    private final int senderThreads;
    private final ConcurrentHashMap<Integer, List<FunctionMessage>> messageMap;//sender threadId -> message list
    private final ConcurrentHashMap<Integer, Integer> encoded_Lengths = new ConcurrentHashMap<>();//threadId -> encoded_length
    public RemoteOperationBatch(int receiverThreads, int senderThreads) {
        this.receiverThreads = receiverThreads;
        this.senderThreads = senderThreads;
        messageMap = new ConcurrentHashMap<>();
        for (int i = 0; i < senderThreads; i++) {
            messageMap.put(i, new ArrayList<>());
            encoded_Lengths.put(i, 0);
        }
    }
    public void addMessage(int senderThreadId, FunctionMessage message) {
        messageMap.get(senderThreadId).add(message);
        encoded_Lengths.put(senderThreadId, encoded_Lengths.get(senderThreadId) + msgEncodeLength(message));
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
        List<FunctionMessage> totalMessages = new ArrayList<>();
        for (int i = 0; i < senderThreads; i++) {
            totalEncodedLength += encoded_Lengths.get(i);
            totalMessages.addAll(messageMap.get(i));
        }
        //START_FLAG(Short) + TotalLength(Int) + MessageBlockLength(Int) * totalThreads + EndFlag(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(2 + 4 + receiverThreads * 4 + totalEncodedLength + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);
            bout.writeInt(totalEncodedLength);
            Deque<Integer> length = new ArrayDeque<>();
            for (int i = 0; i < receiverThreads; i++) {
                int totalLength = 0;
                for (int j = i * totalMessages.size() / receiverThreads; j < (i + 1) * totalMessages.size() / receiverThreads; j ++) {
                    totalLength += totalMessages.get(j).getEncodeLength() + 4;
                }
                length.add(totalLength);
            }
            while (!length.isEmpty()) { //MessageBlockLength(Int) * totalThreads
                bout.writeInt(length.poll());
            }
            for (FunctionMessage msg : totalMessages) {//msg.length(Int) + msg + msg.length(Int) + msg...
                bout.writeInt(msg.getEncodeLength());
                bout.write(msg.message());
            }
            bout.writeShort(SOURCE_CONTROL.END_FLAG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return bout.buffer();
    }
    public void clear() {
        for (int i = 0; i < senderThreads; i++) {
            messageMap.get(i).clear();
            encoded_Lengths.put(i, 0);
        }
    }

    public boolean isEmpty() {
        return messageMap.isEmpty();
    }
}
