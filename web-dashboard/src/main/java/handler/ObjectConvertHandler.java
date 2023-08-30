package handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;
import object.request.AbstractRequest;
import object.request.BasicInfoRequest;
import object.request.DetailedInfoRequest;


/**
 * ObjectConvertHandler converts inbound messages to corresponding requests
 * which will therefore be handled by a specific handler
 */
@Slf4j
public class ObjectConvertHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, TextWebSocketFrame textWebSocketFrame) throws Exception {
        String msg = textWebSocketFrame.text();
        System.out.println(msg);
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

            default:
                throw new RuntimeException("No corresponding request type found");
        }
        channelHandlerContext.fireChannelRead(request);
    }
}
