package intellistream.morphstream.common.io.Rdma.Listener;

import intellistream.morphstream.common.io.Rdma.Channel.RdmaChannel;

import java.net.InetSocketAddress;

public interface RdmaConnectionListener {
    void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) throws Exception;
    void onFailure(Throwable exception); // Must handle multiple calls
}
