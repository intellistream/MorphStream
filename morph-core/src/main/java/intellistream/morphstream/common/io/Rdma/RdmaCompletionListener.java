package intellistream.morphstream.common.io.Rdma;

import java.nio.ByteBuffer;
import java.util.EventListener;

public interface RdmaCompletionListener extends EventListener {
    void onSuccess(ByteBuffer buffer);

    void onSuccess(ByteBuffer buffer);

    void onFailure(Throwable exception); // must handle multiple calls
}
