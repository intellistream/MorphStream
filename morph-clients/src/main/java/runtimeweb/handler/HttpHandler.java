package runtimeweb.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.*;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
    private static final Map<String, FileUpload> fileUploads = new HashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest request) throws Exception {
        String uri = request.uri();
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        String path = queryStringDecoder.path();    // get the query path

        if (path.equals("/upload")) {
            // jar upload
            try {
                HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(factory, request);
                List<InterfaceHttpData> bodyHttpDatas = decoder.getBodyHttpDatas();

                for (InterfaceHttpData data: bodyHttpDatas) {
                    if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
                        FileUpload fileUpload = (FileUpload) data;
                        saveUploadedFile(fileUpload);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set("Content-Type", "text/plain; charset=UTF-8");
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE");
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, Authorization, x-requested-with");
            response.setStatus(HttpResponseStatus.OK);
            response.content().writeBytes("File received".getBytes(CharsetUtil.UTF_8));

            channelHandlerContext.writeAndFlush(response);
            channelHandlerContext.close();
        }
    }

    private void saveUploadedFile(FileUpload fileUpload) throws IOException {
        if (fileUpload.isCompleted()) {
            String fileName = fileUpload.getFilename();
            ByteBuf content = fileUpload.content();
            String saveDirectory = "C:\\Users\\siqxi\\data\\cache\\test";
            File file = new File(saveDirectory, fileName);
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                while (content.isReadable()) {
                    byte[] buffer = new byte[content.readableBytes()];
                    content.readBytes(buffer);
                    fileOutputStream.write(buffer);
                }
            }
        }
    }
}
