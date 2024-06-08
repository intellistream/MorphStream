package intellistream.morphstream.common.io.Rdma.RdmaUtils;

import lombok.Getter;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SOURCE_CONTROL {
    private static final SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    public static short START_FLAG = 0x6FFF;
    public static short END_FLAG = 0x7FFF;
    public static short FINISH_FLAG = 0x5FFF;
    public static int SHARED_LOCK = 0x0400;
    private CyclicBarrier driverStartBarrier;
    private CyclicBarrier driverEndBarrier;
    private CyclicBarrier workerStartBarrier;
    private CyclicBarrier workerEndBarrier;
    @Getter
    private int messagePerFrontend;
    @Getter
    private int resultPerExecutor;
    public void config(int number_frontends, int messagePerFrontend, int number_executor, int resultPerExecutor) {
        driverStartBarrier = new CyclicBarrier(number_frontends);
        driverEndBarrier = new CyclicBarrier(number_frontends);
        workerStartBarrier = new CyclicBarrier(number_executor);
        workerEndBarrier = new CyclicBarrier(number_executor);
        this.messagePerFrontend = messagePerFrontend;
        this.resultPerExecutor = resultPerExecutor;
    }

    public void driverStartSendMessageBarrier() {
        try {
            driverStartBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
    public void driverEndSendMessageBarrier() {
        try {
            driverEndBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
    public void workerStartSendResultBarrier() {
        try {
            workerStartBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
    public void workerEndSendResultBarrier() {
        try {
            workerEndBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    public static SOURCE_CONTROL getInstance() {
        return ourInstance;
    }

}
