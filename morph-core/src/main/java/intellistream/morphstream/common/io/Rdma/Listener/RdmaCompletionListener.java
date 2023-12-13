package intellistream.morphstream.common.io.Rdma.Listener;

import java.nio.ByteBuffer;
import java.util.EventListener;

public interface RdmaCompletionListener extends EventListener {
    void onSuccess(ByteBuffer buffer, Integer imm);

    void onFailure(Throwable exception); // must handle multiple calls
}
