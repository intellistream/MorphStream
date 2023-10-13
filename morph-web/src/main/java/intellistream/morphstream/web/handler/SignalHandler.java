package intellistream.morphstream.web.handler;
import intellistream.morphstream.web.common.request.SignalRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.Getter;
import lombok.Setter;

@Getter
@ChannelHandler.Sharable
public class SignalHandler extends SimpleChannelInboundHandler<SignalRequest> {
    @Getter
    ChannelHandlerContext context;  // the context of the channel
    @Setter
    SignalType signalType = null;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.context = ctx;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, SignalRequest signalRequest) throws Exception {
        if (signalRequest.getSignal().equals("start")) {
            this.signalType = SignalType.START;
        } else if (signalRequest.getSignal().equals("stop")) {
            this.signalType = SignalType.STOP;
        } else {
            throw new RuntimeException("Invalid control signal: " + signalRequest.getSignal());
        }
    }

    public enum SignalType {
        START, STOP
    }
}
