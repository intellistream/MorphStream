package intellistream.morphstream.web.handler.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import intellistream.morphstream.web.common.dao.BatchRuntimeData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import sun.rmi.runtime.Log;

public class BatchInfoSender extends ChannelOutboundHandlerAdapter {
    ChannelHandlerContext context;  // the context of the channel
    ObjectMapper objectMapper;

    public BatchInfoSender() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.context = ctx;
    }

    public void send(BatchRuntimeData message) {
        if (this.context != null) {
            try {
                System.out.println(objectMapper.writeValueAsString(message));
                String a = objectMapper.writeValueAsString(message);
                this.context.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(message)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }
}
