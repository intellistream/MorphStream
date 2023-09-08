package handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import lombok.extern.slf4j.Slf4j;
import object.Response;
import object.request.DetailedInfoRequest;
import object.response.DetailedInfoResponse;

import java.io.File;
import java.io.IOException;

@Slf4j
@ChannelHandler.Sharable
public class DetailedInfoHandler extends SimpleChannelInboundHandler<DetailedInfoRequest> {
    private final String PATH = "C:\\Users\\siqxi\\data\\job\\";  // TODO: Extract this to Config

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DetailedInfoRequest request) throws Exception {
        String appId = request.getAppId();
        String correlationId = request.getCorrelationId();
        log.debug("New request received => Detailed info request for id: {}", appId);

        // retrieving detailed historical data
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            DetailedInfoResponse detailedInfoResponse = objectMapper.readValue(new File(PATH + appId + ".json"), DetailedInfoResponse.class);   // Convert a JSON to a DetailedInfoResponse
            Response<DetailedInfoResponse> response = new Response<>();
            response.setData(detailedInfoResponse);
            response.setType("response");
            response.setCorrelationId(correlationId);
            System.out.println(objectMapper.writeValueAsString(response));
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
