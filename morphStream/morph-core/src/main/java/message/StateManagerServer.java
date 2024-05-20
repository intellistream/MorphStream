package message;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
// import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class StateManagerServer
{
    private static final Logger logger = Logger.getLogger(StateManagerServer.class.getName());
    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new StateManagerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown
                // hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    StateManagerServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon
     * threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        final StateManagerServer server = new StateManagerServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class StateManagerImpl extends message.StateManagerGrpc.StateManagerImplBase {
        @Override
        public void monitorReport(message.MonitorReportMessage request,
                                  io.grpc.stub.StreamObserver<message.Empty> responseObserver) {
            System.out.println("Monitor Report received CC: " + request.getCcValue());
            // private message.Empty t = message.Empty.newBuilder().build();
            // responseObserver.onNext(t);
            // responseObserver.onNext(reply);
            // responseObserver.onCompleted();
        }

        public void txnRequest(message.TxnReqMessage request,
                               io.grpc.stub.StreamObserver<message.Empty> responseObserver) {
            System.out.println("StateManager received txnRequest id: " + request.getId());
        }

        /**
         */
        public void sFC(message.SFCMessage request,
                        io.grpc.stub.StreamObserver<message.Empty> responseObserver) {
            System.out.println("StateManager received CC: " + request.getSFCJson());
        }
    }
}
