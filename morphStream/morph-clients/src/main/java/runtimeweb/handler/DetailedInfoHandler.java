package runtimeweb.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.Response;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import runtimeweb.common.request.DetailedInfoRequest;
import runtimeweb.common.response.DetailedInfoResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.io.File;
import java.io.IOException;

@ChannelHandler.Sharable
public class DetailedInfoHandler extends SimpleChannelInboundHandler<DetailedInfoRequest> {
    private final String PATH = MorphStreamEnv.get().configuration().getString("dataPath", "morphStream/data/jobs");

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DetailedInfoRequest request) throws Exception {
        String appId = request.getAppId();
        String correlationId = request.getCorrelationId();

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
