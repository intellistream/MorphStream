package intellistream.morphstream.web.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import intellistream.morphstream.web.common.request.AbstractRequest;
import intellistream.morphstream.web.common.request.BasicInfoRequest;
import intellistream.morphstream.web.common.request.DetailedInfoRequest;
import intellistream.morphstream.web.common.request.SignalRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;


/**
 * ObjectConvertHandler converts inbound messages to corresponding requests
 * which will therefore be handled by a specific handler
 */
public class ObjectConvertHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, TextWebSocketFrame textWebSocketFrame) throws Exception {
        String msg = textWebSocketFrame.text();
        JsonNode rootNode = objectMapper.readTree(msg);

        String type = rootNode.get("type").asText();
        AbstractRequest request;

        // request type casting
        switch (type) {
            case "BasicInfoRequest":
                request = objectMapper.treeToValue(rootNode, BasicInfoRequest.class);
                break;
            case "DetailInfoRequest":
                request = objectMapper.treeToValue(rootNode, DetailedInfoRequest.class);
                break;
            case "Signal":
                request = objectMapper.treeToValue(rootNode, SignalRequest.class);
                break;
            default:
                throw new RuntimeException("No corresponding request type found");
        }
        channelHandlerContext.fireChannelRead(request);
    }
}
