package intellistream.morphstream.web.handler.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

public class BatchInfoSender extends ChannelOutboundHandlerAdapter {
    ChannelHandlerContext context;  // the context of the channel
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.context = ctx;
    }

    public void send(String message) {
        if (this.context != null) {
            this.context.writeAndFlush(new TextWebSocketFrame(message));
        }
    }
}
