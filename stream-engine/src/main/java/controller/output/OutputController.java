package controller.output;

import common.util.datatypes.StreamValues;
import components.context.TopologyContext;
import execution.runtime.collector.impl.MetaGroup;
import execution.runtime.tuple.impl.Marker;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

/**
 * OutputController may or maynot be shared by multiple producers.
 */
public abstract class OutputController implements Serializable {
    private static final long serialVersionUID = 31L;
    private boolean shared = false;

    OutputController() {
    }

    public boolean isShared() {
        return shared;
    }

    public void setShared() {
        this.shared = true;
    }

    public abstract PartitionController getPartitionController(String streamId, String boltID);

    public abstract Collection<PartitionController> getPartitionController();

    public abstract Collection<PartitionController> getPartitionController(String StreamId);

    public abstract boolean isEmpty();

    /**
     * Initilize output queue for each partition partition.
     *
     * @param linked
     * @param desired_elements_epoch_per_core
     */
    public abstract void allocatequeue(boolean linked, int desired_elements_epoch_per_core);

    /**
     * As OutputController is shared, we need to know ``who is sending the tuple"
     *
     * @param MetaGroup
     * @param streamId
     * @param bid
     * @param data
     */
    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... data) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object data) throws InterruptedException;

    public abstract void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException;

    public abstract void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... data) throws InterruptedException;

    public abstract void force_emitOnStream(MetaGroup MetaGroup, String streamId, long[] bid, long msg_id, Object... data) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException;

    //	public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, AbstractLRBTuple data) throws InterruptedException;
    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException;

    public abstract void force_emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data, long bid, long timestamp) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] data) throws InterruptedException;

    public abstract void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] key, long value) throws InterruptedException;

    public abstract void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object... output) throws InterruptedException;

    public abstract void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object output) throws InterruptedException;

    public abstract void emitOnStream_bid(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException;

    /**
     * As OutputController is shared, we need to know ``who is sending the tuple"
     *
     * @param MetaGroup
     * @param streamId
     * @param bid
     * @param gap
     * @param data
     */
    public abstract void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, Object... data) throws InterruptedException;

    public abstract void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, char[] data) throws InterruptedException;

    public abstract void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException;

    public abstract void emitOnStream_inorder_single(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException;

    public abstract void emitOnStream_inorder_push(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap) throws InterruptedException;

    //	public abstract void emitMarkedOnStream(String streamId, Object... data, long bid);
//
//
//	public abstract void emitOnStreamWithMark(String streamId, Object... data, Marker marker);
//
    public abstract void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, Object... data);

    public abstract void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] key, long value);

    public abstract void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException;

    public abstract void create_marker_single(MetaGroup meta, long boardcast_time, String streamId, long bid, int myiteration);

    /**
     * create and boardcast the Marker
     *
     * @param MetaGroup
     * @param timestamp the timestamp of creating the marker
     * @param bid
     */
    public abstract void create_marker_boardcast(MetaGroup MetaGroup, long timestamp, long bid, int myitration) throws InterruptedException;

    public abstract void create_marker_boardcast(MetaGroup meta, long boardcast_time, String streamId, long bid, int myiteration) throws InterruptedException;

    public boolean isUnique() {
        return !shared;
    }

    /**
     * simply forward the Marker
     *
     * @param MetaGroup
     * @param marker
     */
    public abstract void marker_boardcast(MetaGroup MetaGroup, long bid, Marker marker) throws InterruptedException;

    public abstract void marker_boardcast(MetaGroup MetaGroup, String streamId, long bid, Marker marker) throws InterruptedException;

    public abstract void setContext(int executorID, TopologyContext context);

    //	public abstract void try_fill_gap(String streamId);
    public abstract long getBID(String streamId);
//	public abstract void increaseGap(String streamId);
//	public abstract void addGap(String streamId, long bid);
//    public abstract void emitOnStream(String streamId, JumboTuple data, JumboTuple input);
//    public abstract void emitOnStream_nowait(String streamId, JumboTuple data, JumboTuple input);
}
