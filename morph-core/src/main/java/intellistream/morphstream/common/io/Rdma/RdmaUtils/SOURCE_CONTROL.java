package intellistream.morphstream.common.io.Rdma.RdmaUtils;

import lombok.Getter;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SOURCE_CONTROL {
    private static final SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    private CyclicBarrier startBarrier;
    private CyclicBarrier endBarrier;
    @Getter
    private int messagePerFrontend;
    public void config(int number_frontends, int messagePerFrontend) {
        startBarrier = new CyclicBarrier(number_frontends);
        endBarrier = new CyclicBarrier(number_frontends);
        this.messagePerFrontend = messagePerFrontend;
    }

    public void startSendBarrier() {
        try {
            startBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
    public void endSendBarrier() {
        try {
            endBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    public static SOURCE_CONTROL getInstance() {
        return ourInstance;
    }

}
