package runtimeweb.handler;
import cli.SLClient;
import cli.WebServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.Response;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtimeweb.common.request.SignalRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import runtimeweb.common.response.SignalResponse;

@ChannelHandler.Sharable
public class SignalHandler extends SimpleChannelInboundHandler<SignalRequest> {
    private static final Logger log = LoggerFactory.getLogger(SignalHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, SignalRequest request) throws Exception {
        String appId = request.getAppId();
        String correlationId = request.getCorrelationId();

        if (request.getSignal().equals("start")) {
            log.info("Start a new job");
            Response<SignalResponse> response = generateResponse(appId, correlationId, true);
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
            // start the job
            WebServer.createJobInfoJSON("StreamLedger");
            SLClient.startJob(new String[]{});
        } else if (request.getSignal().equals("stop")) {
            log.info("Stop signal received");
//            TPGInputListener.get().insertStopSignal(); // notify spout to pass stop signal downstream
            Response<SignalResponse> response = generateResponse(appId, correlationId, false);
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
        } else {
            throw new RuntimeException("Invalid control signal: " + request.getSignal());
        }
    }

    public Response<SignalResponse> generateResponse(String appId, String correlationId, boolean isStart) {
        Response<SignalResponse> response = new Response<>();
        SignalResponse signalResponse;
        if (isStart) {
            // generate start reply
            signalResponse = new SignalResponse(appId, true);
        } else {
            // generate stop reply
            signalResponse = new SignalResponse(appId, true);
        }
        response.setData(signalResponse);
        response.setType("response");
        response.setCorrelationId(correlationId);
        return response;
    }
}
