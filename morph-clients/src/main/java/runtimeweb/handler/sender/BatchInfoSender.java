package runtimeweb.handler.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.BatchRuntimeData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.Getter;

public class BatchInfoSender extends ChannelOutboundHandlerAdapter {
    @Getter
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
                this.context.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(message)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }
}
