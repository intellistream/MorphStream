package sesame.components.operators.api;

import sesame.execution.runtime.tuple.impl.Marker;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {

    boolean checkpoint(int counter) throws InterruptedException, BrokenBarrierException;

    void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException;

    void forward_checkpoint_single(int sourceId, long bid, Marker marker) throws InterruptedException;

    void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException;

    void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException;

    /**
     * Optionally relax_reset state before marker.
     *
     * @param marker
     */
    void ack_checkpoint(Marker marker);


    void earlier_ack_checkpoint(Marker marker);
}
