package runtimeweb.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import communication.dao.Batch;
import communication.dao.Response;
import intellistream.morphstream.engine.txn.profiler.RuntimeMonitor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runtimeweb.common.request.PerformanceRequest;

public class PerformanceHandler  extends SimpleChannelInboundHandler<PerformanceRequest> {
    private static final Logger log = LoggerFactory.getLogger(SignalHandler.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, PerformanceRequest performanceRequest) throws Exception {
        Batch data = RuntimeMonitor.getBatchedDataByBatch(performanceRequest.getLatestBatch(), performanceRequest.getOperator());
        if (data != null) {
            Response<Batch> response = new Response<>();
            response.setData(data);
            response.setType("performance");
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(objectMapper.writeValueAsString(response)));
        }
    }
}
