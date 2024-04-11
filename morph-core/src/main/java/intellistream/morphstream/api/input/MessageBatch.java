package intellistream.morphstream.api.input;

import intellistream.morphstream.common.io.Rdma.Memory.Buffer.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MessageBatch {
    private final int receiverThreads;
    private final int senderThreads;
    private int buffer_size;
    private final ConcurrentHashMap<Integer, List<FunctionMessage>> msgs = new ConcurrentHashMap<>();//threadId -> List<FunctionMessage>
    private final ConcurrentHashMap<Integer, Integer> encoded_Lengths = new ConcurrentHashMap<>();//threadId -> encoded_length
    // START_FLAG(Short) + TotalLength(Int) +  MessageBlockLength(Int) * totalThreads + msg.length(Int) + msg + msg.length(Int) + msg... + EndFlag(Short)
    public MessageBatch(int buffer_size, int receiverThreads, int senderThreads) {
        this.buffer_size = buffer_size;
        this.receiverThreads = receiverThreads;
        this.senderThreads = senderThreads;
    }
    public void add(int senderThread, FunctionMessage msg) {
        if (msg == null)
            throw new RuntimeException("null object forbidden in message batch");
        msgs.get(senderThread).add(msg);
        encoded_Lengths.put(senderThread, encoded_Lengths.get(senderThread) + msgEncodeLength(msg));
    }
    private int msgEncodeLength(FunctionMessage workerMessage) {
        if (workerMessage == null) return 0;

        int size = 4; //INT
        if (workerMessage.message() != null)
            size += workerMessage.getEncodeLength();
        return size;
    }
    /**
     * @return true if this batch used up allowed buffer size
     */
    public boolean isFull() {
        return msgs.size() >= buffer_size;
    }
    /**
     * @return true if this batch doesn't have any messages
     */
    public boolean isEmpty() {
        return msgs.isEmpty();
    }
    /**
     * @return number of msgs in this batch
     */
    public int size() {
        return msgs.size();
    }
    public ByteBuffer buffer() {
        int totalEncodedLength = 0;
        List<FunctionMessage> totalMessage = new ArrayList<>();
        for (int i = 0; i < senderThreads; i++) {
            totalEncodedLength += encoded_Lengths.get(i);
        }
        for (int i = 0; i < senderThreads; i++) {
            totalMessage.addAll(msgs.get(i));
        }
        //START_FLAG(Short) + TotalLength(Int) + MessageBlockLength(Int) * totalThreads + EndFlag(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(2 + 4 + receiverThreads * 4 + totalEncodedLength + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);//START_FLAG(Short)
            bout.writeInt(totalEncodedLength);//TotalLength(Int)
            Deque<Integer> length = new ArrayDeque<>();
            for (int i = 0; i < receiverThreads; i++) {
                int totalLength = 0;
                for (int j = i * totalMessage.size() / receiverThreads; j < (i + 1) * totalMessage.size() / receiverThreads; j++) {
                    totalLength += totalMessage.get(j).getEncodeLength() + 4;
                }
                length.add(totalLength);
            }
            while (length.size() > 0) { //MessageBlockLength(Int) * totalThreads
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
    public void clear() {
        for (int i = 0; i < senderThreads; i++) {
            msgs.get(i).clear();
            encoded_Lengths.put(i, 0);
        }
    }
}
