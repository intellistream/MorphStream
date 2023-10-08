package intellistream.morphstream.web;

import intellistream.morphstream.web.handler.BasicInfoHandler;
import intellistream.morphstream.web.handler.DetailedInfoHandler;
import intellistream.morphstream.web.handler.HttpHandler;
import intellistream.morphstream.web.handler.ObjectConvertHandler;
import intellistream.morphstream.web.handler.sender.BatchInfoSender;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.Getter;

/**
 * WebSocketHandler contains and constructs all handlers needed in the channel pipeline
 * This is the main handler
 */
@Getter
public class WebSocketHandler extends ChannelInitializer<SocketChannel> {
    BatchInfoSender batchInfoSender = new BatchInfoSender();

    @Override
    protected void initChannel(SocketChannel channel) {
        this.batchInfoSender = new BatchInfoSender();

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
                .addLast("http-handler", new HttpHandler())
                // sender
                .addLast("batch-info-sender", this.batchInfoSender);
    }
}
