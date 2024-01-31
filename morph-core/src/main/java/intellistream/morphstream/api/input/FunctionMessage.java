package intellistream.morphstream.api.input;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Used to send function message(in byte) to the worker.
 */
public class FunctionMessage {
    private byte[] _message;
    public FunctionMessage(String message) {
        this._message = message.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length);
        bb.put(_message);
        return bb;
    }
    public void deserialize(ByteBuffer packet) {
        if (packet == null) return;
        _message = new byte[packet.limit()];
        packet.get(_message);
    }

    public byte[] message() {
        return _message;
    }

    public int getEncodeLength() {
        return _message.length;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FunctionMessage that = (FunctionMessage) obj;
        return Arrays.equals(_message, that._message);
    }
}
