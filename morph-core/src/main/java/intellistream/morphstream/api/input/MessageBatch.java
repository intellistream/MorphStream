package intellistream.morphstream.api.input;

import intellistream.morphstream.common.io.Rdma.Memory.ByteBufferBackedOutputStream;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.SOURCE_CONTROL;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

public class MessageBatch {
    private final Object writeLock = new Object();
    private final int totalThreads;
    private int buffer_size;
    private ArrayList<FunctionMessage> msgs;
    private int encoded_length;//Total size in bytes of all messages in this batch,
    // START_FLAG(Short) + TotalLength(Int) +  MessageBlockLength(Int) * totalThreads + msg.length(Int) + msg + msg.length(Int) + msg... + EndFlag(Short)
    public MessageBatch(int buffer_size, int totalThreads) {
        this.buffer_size = buffer_size;
        msgs = new ArrayList<>();
        encoded_length = 0;
        this.totalThreads = totalThreads;
    }
    public void add(FunctionMessage msg) {
        if (msg == null)
            throw new RuntimeException("null object forbidden in message batch");
        msgs.add(msg);
        encoded_length += msgEncodeLength(msg);
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
    public Object getWriteLock() {
        return writeLock;
    }
    public ByteBuffer buffer() {
        //START_FLAG(Short) + TotalLength(Int) + MessageBlockLength(Int) * totalThreads + EndFlag(Short)
        ByteBufferBackedOutputStream bout = new ByteBufferBackedOutputStream(ByteBuffer.allocate(2 + 4 + totalThreads * 4 + encoded_length + 2));
        try {
            bout.writeShort(SOURCE_CONTROL.START_FLAG);//START_FLAG(Short)
            bout.writeInt(encoded_length);//TotalLength(Int)
            Deque<Integer> length = new ArrayDeque<>();
            for (int i = 0; i < totalThreads; i++) {
                int totalLength = 0;
                for (int j = i * msgs.size() / totalThreads; j < (i + 1) * msgs.size() / totalThreads; j++) {
                    totalLength += msgs.get(j).getEncodeLength() + 4;
                }
                length.add(totalLength);
            }
            while (length.size() > 0) { //MessageBlockLength(Int) * totalThreads
                bout.writeInt(length.poll());
            }
            for (FunctionMessage msg : msgs) {//msg.length(Int) + msg + msg.length(Int) + msg...
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
        msgs.clear();
        encoded_length = 0;
    }
}
