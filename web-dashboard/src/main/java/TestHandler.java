import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

@Slf4j
public class TestHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, TextWebSocketFrame textWebSocketFrame) throws Exception {
        // log the message received
        log.debug("Message received: {}", textWebSocketFrame.text());

        // 返回消息给客户端
        channelHandlerContext.writeAndFlush(new TextWebSocketFrame("服务器时间：" + LocalDateTime.now() + " : " + textWebSocketFrame.text()));
    }
}
