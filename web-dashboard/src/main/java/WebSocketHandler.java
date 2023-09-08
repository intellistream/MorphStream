import handler.BasicInfoHandler;
import handler.DetailedInfoHandler;
import handler.HttpHandler;
import handler.ObjectConvertHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * WebSocketHandler contains and constructs all handlers needed in the channel pipeline
 * This is the main handler
 */
public class WebSocketHandler extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        channel.pipeline()
                // Websocket config
                .addLast("http-codec", new HttpServerCodec())
                .addLast("aggregator", new HttpObjectAggregator(1024 * 1024))
                .addLast("chunked-writer", new ChunkedWriteHandler())
                .addLast("protocolHandler", new WebSocketServerProtocolHandler("/websocket"))
                .addLast("object-convertor", new ObjectConvertHandler())
                // self-defined handlers
                .addLast("basic-info-handler", new BasicInfoHandler())
                .addLast("detailed-info-handler", new DetailedInfoHandler())

                // http handlers
                .addLast("http-handler", new HttpHandler());
    }
}
