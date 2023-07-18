package engine.stream.components.operators.api;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {
    boolean model_switch(int counter) throws InterruptedException, BrokenBarrierException;
}
