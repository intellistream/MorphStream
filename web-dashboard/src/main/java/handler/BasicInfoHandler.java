package handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;
import object.request.BasicInfoRequest;
import object.response.BasicInfoResponse;

import java.io.File;
import java.io.IOException;

@Slf4j
@ChannelHandler.Sharable
public class BasicInfoHandler extends SimpleChannelInboundHandler<BasicInfoRequest> {
    private final String PATH = "C:\\Users\\siqxi\\data\\stats\\test_data.json";


    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, BasicInfoRequest request) throws Exception {
        String appId = request.getAppId();
        String correlationId = request.getCorrelationId();

        log.debug("New request received => Basic info request for id: {}", appId);

        // retrieving data
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // Convert a JSON to an object
            BasicInfoResponse response = objectMapper.readValue(new File(PATH), BasicInfoResponse.class);
            response.setCorrelationId(correlationId);

            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
            System.out.println("Data sent: " + response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
