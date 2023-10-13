//package intellistream.morphstream.web;
//
//import io.netty.bootstrap.ServerBootstrap;
//import io.netty.channel.Channel;
//import io.netty.channel.EventLoopGroup;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.nio.NioServerSocketChannel;
//
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
//
///**
// * HistoricalServer is the backend server that retrieves all information about finished applications
// *
//*/
//public class HistoricalServer {
//    public static void main(String[] args) {
//        start();
//    }
//
//    public static void start() {
//        EventLoopGroup bossGroup = new NioEventLoopGroup();
//        EventLoopGroup workerGroup = new NioEventLoopGroup(2);
//        WebSocketHandler webSocketHandler = new WebSocketHandler();
//
//        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
//
//        try {
//            ServerBootstrap bootstrap = new ServerBootstrap();
//            bootstrap.group(bossGroup, workerGroup)
//                    .channel(NioServerSocketChannel.class)
//                    .childHandler(webSocketHandler);
//            Channel channel = bootstrap.bind(5001).sync().channel();
//
//            service.scheduleAtFixedRate(()->{
////                webSocketHandler.getBatchInfoSender().send("{\"type\": \"Performance\", \"jobId\": 1}");
//            }, 10, 1, TimeUnit.SECONDS);
//            channel.closeFuture().sync(); // block until server is closed
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            bossGroup.shutdownGracefully();
//            workerGroup.shutdownGracefully();
//        }
//    }
//}
