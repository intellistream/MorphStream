package intellistream.morphstream.api.input.java_peer.message;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class VNFCtrlClient {
  private static final Logger logger = Logger.getLogger(VNFCtrlClient.class.getName());

  private final VNFCtrlGrpc.VNFCtrlBlockingStub blockingStub;

  /** Construct client for accessing HelloWorld server using the existing channel. */
  public VNFCtrlClient(Channel channel) {
    // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
    // shut it down.

    // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
    blockingStub = VNFCtrlGrpc.newBlockingStub(channel);
  }

  public void make_pause() {
    logger.info("Trying to make VNF pause ...");
    message.Empty request = Empty.newBuilder().build();
    try {
      blockingStub.pause(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
  }

  public void make_continue() {
    logger.info("Trying to make VNF pause ...");
    message.Empty request = Empty.newBuilder().build();
    try {
      blockingStub.continue_(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
  }

  public void update_cc(int key, CC cc) {
    logger.info("Trying to update cc ...");
    message.CCMessage request = message.CCMessage.newBuilder()
      .setCc(cc)
      .setKey(key)
      .build();
    try {
      blockingStub.postCC(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
  }

  public void fetch_cc(int key) {
    logger.info("Trying to fetch cc ...");
    message.CCMessage request = message.CCMessage.newBuilder()
      .setKey(key)
      .build();
    message.CCMessage response;
    try {
      response = blockingStub.getCC(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    System.out.println("CC received: " + response.getCc());
  }

  public void update_value(int key, int value) {
    logger.info("Trying to update value ...");
    message.DSMessage request = message.DSMessage.newBuilder()
      .setKey(key)
      .setValue(value)
      .build();
    try {
      blockingStub.postValue(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
  }

  public void fetch_value(int key) {
    logger.info("Trying to fetch value ...");
    message.DSMessage request = message.DSMessage.newBuilder()
      .setKey(key)
      .build();
    message.DSMessage response;
    try {
      response = blockingStub.getValue(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    System.out.println("Value received: " + response.getValue());
  }

  public void execute_sa_udf(long pktId, int saIdx, int key, int value) {
    logger.info("Trying to update cc ...");
    message.UDFMessage request = message.UDFMessage.newBuilder()
      .setId(pktId)
      .setKey(key)
      .setSaIdx(saIdx)
      .setValue(value)
      .build();
    message.UDFResponse response;
    try {
      response = blockingStub.uDFReady(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    System.out.println("Value to write received: " + response.getValue());
  }

  public static void main(String[] args) throws Exception {
    String user = "world";
    // Access a service running on the local machine on port 50051
    String target = "localhost:50051";
    // Allow passing in the user and target strings as command line arguments
    if (args.length > 0) {
      if ("--help".equals(args[0])) {
        System.err.println("Usage: [name [target]]");
        System.err.println("");
        System.err.println("  name    The name you wish to be greeted by. Defaults to " + user);
        System.err.println("  target  The server to connect to. Defaults to " + target);
        System.exit(1);
      }
      user = args[0];
    }
    if (args.length > 1) {
      target = args[1];
    }

    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
        .build();
    try {
      VNFCtrlClient client = new VNFCtrlClient(channel);
      client.fetch_cc(123);
    } finally {
      // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
      // resources the channel should be shut down when it will no longer be used. If it may be used
      // again leave it running.
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
} 