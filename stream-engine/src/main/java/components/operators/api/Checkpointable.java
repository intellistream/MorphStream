package components.operators.api;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {
    boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException;
}
