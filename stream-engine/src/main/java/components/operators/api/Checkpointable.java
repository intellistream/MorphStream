package components.operators.api;

import execution.runtime.tuple.impl.Marker;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {
    boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException;

    /**
     * Optionally relax_reset state before marker.
     *
     * @param marker
     */
    void ack_checkpoint(Marker marker);

}
