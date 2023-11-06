package runtimeweb.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.Response;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import runtimeweb.common.request.BasicInfoRequest;
import runtimeweb.common.response.BasicInfoResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;


import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ChannelHandler.Sharable
public class BasicInfoHandler extends SimpleChannelInboundHandler<BasicInfoRequest> {
    private final String PATH = MorphStreamEnv.get().configuration().getString("dataPath", "morphStream/data/jobs");

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, BasicInfoRequest request) throws Exception {
        String appId = request.getAppId();
        String correlationId = request.getCorrelationId();

        // retrieving basic historical data
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            if (Integer.parseInt(appId) != 0) {
                // Retrieve single job data
                BasicInfoResponse basicInfoResponse = objectMapper.readValue(new File(PATH + appId + ".json"), BasicInfoResponse.class);   // Convert a JSON to a BasicInfoResponse
                Response<BasicInfoResponse> response = new Response<>();
                response.setType("response");
                response.setCorrelationId(correlationId);
                response.setData(basicInfoResponse);
                channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
            } else {
                // Retrieve all historical jobs data
                Response<List<BasicInfoResponse>> response = new Response<>();
                response.setType("response");
                response.setCorrelationId(correlationId);
                response.setData(new ArrayList<>());

                File directory = new File(PATH);
                if (directory.exists() && directory.isDirectory()) {
                    FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
                    File[] jsonFiles = directory.listFiles(jsonFilter);
                    for (File jsonFile: jsonFiles) {
                        BasicInfoResponse singleAppRes = objectMapper.readValue(jsonFile, BasicInfoResponse.class);   // Convert a JSON to an object
                        response.getData().add(singleAppRes);
                    }
                    channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
