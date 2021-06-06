package execution.runtime.collector;

import common.collections.OsUtils;
import common.util.datatypes.StreamValues;
import components.TopologyComponent;
import components.context.TopologyContext;
import controller.output.OutputController;
import execution.ExecutionNode;
import execution.runtime.collector.impl.Meta;
import execution.runtime.collector.impl.MetaGroup;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Set;

import static common.CONTROL.enable_debug;
import static common.Constants.DEFAULT_STREAM_ID;

/**
 * Created by shuhaozhang on 13/7/16.
 * <p>
 * OutputCollector is unique to each executor!
 */
public class OutputCollector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OutputCollector.class);
    public final OutputController sc;
    private final ExecutionNode executor;
    private final MetaGroup meta;
    private final int totalEvents;
    private final long last_emit = 0;
    private boolean no_wait = true;
    private long bid_counter;

    public OutputCollector(ExecutionNode executor, TopologyContext context, int TotalEvents) {
        totalEvents = TotalEvents;
        int taskId = context.getThisTaskIndex();
        this.executor = executor;
        this.sc = executor.getController();
        this.meta = new MetaGroup(taskId);
//		Set<TopologyComponent> childrenOP = this.executor.getChildren().keySet();
        for (TopologyComponent childrenOP : this.executor.getChildren().keySet()) {
            this.meta.put(childrenOP, new Meta(taskId));
        }
        long bid_start = (int) (1E9 * (taskId));
        long bid_end = (int) (1E9 * (taskId + 1));
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }


    }

    public boolean isNo_wait() {
        return no_wait;
    }

    public void setNo_wait(boolean no_wait) {
        this.no_wait = no_wait;
    }

    private void forwardResult_inorder(String streamId, long bid, LinkedList<Long> gap, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream_inorder(meta, streamId, bid, gap, data);
    }

    private void forwardResult_inorder(String streamId, long bid, LinkedList<Long> gap, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream_inorder(meta, streamId, bid, gap, data);
    }

    private void forwardResult_inorder(String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        assert tuple != null && sc != null;
        sc.emitOnStream_inorder(meta, streamId, bid, gap, tuple);
    }

    private void forwardResult_inorder_single(String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        assert tuple != null && sc != null;
        sc.emitOnStream_inorder_single(meta, streamId, bid, gap, tuple);
    }

    private void forwardResult_inorder_push(String streamId, long bid, LinkedList<Long> gap) throws InterruptedException {
        assert sc != null;
        sc.emitOnStream_inorder(meta, streamId, bid, gap);
    }

    public void emit_inorder(long bid, LinkedList<Long> gap, Object... values) {
        try {
            forwardResult_inorder(DEFAULT_STREAM_ID, bid, gap, values);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void emit_inorder(long bid, LinkedList<Long> gap, char[] values) {
        try {
            forwardResult_inorder(DEFAULT_STREAM_ID, bid, gap, values);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void emit_inorder_single(String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        try {
            forwardResult_inorder_single(streamId, bid, gap, tuple);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param streamId
     * @param bid
     * @param gap
     * @param values
     * @return
     */
    public void emit_inorder(String streamId, long bid, LinkedList<Long> gap, Object... values) {
        try {
            forwardResult_inorder(streamId, bid, gap, values);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void emit_inorder(String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) {
        try {
            forwardResult_inorder(streamId, bid, gap, tuple);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void emit_inorder_push(String streamId, long bid, LinkedList<Long> gap) {
        try {
            forwardResult_inorder_push(streamId, bid, gap);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param streamId
     * @param bid
     * @param data
     * @return
     */
    public void emit(String streamId, long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }

    public void emit(String streamId, long bid, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }

    public void emit_force(String streamId, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, -1, data);
    }

    public void emit_force(StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, DEFAULT_STREAM_ID, -1, data);
    }

    public void emit(String streamId, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, -1, data);
    }

    public void emit(String data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, DEFAULT_STREAM_ID, -1, data);
    }

    public void emit(StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, DEFAULT_STREAM_ID, -1, data);
    }

    public void emit(String streamId, long bid, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }

    public void emit(String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        assert sc != null;
        sc.emitOnStream(meta, streamId, bid, deviceID, movingAvergeInstant, nextDouble);
    }

    public void emit(String streamId, long bid, char[] key, long value) throws InterruptedException {
        assert key != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, key, value);
    }

    public void emit(String streamId, long bid, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, bid, data);
    }

    public void force_emit(String streamId, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, -1, data);
    }

    public void force_emit(long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, DEFAULT_STREAM_ID, bid, data);
    }

    public void force_emit(String streamId, long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data);
    }

    public void force_emit(String streamId, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, data);
    }

    public void emit(String streamId, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, data);
    }

    public void emit(String streamId, char[] data, long bid, long timestamp) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, data, bid, timestamp);
    }

    /**
     * @param streamId
     * @param data
     * @return
     */
    public void emit_bid(String streamId, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream_bid(meta, streamId, data);
    }

    public void emit_bid(String streamId, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream_bid(meta, streamId, data);
    }

    public void emit_bid(char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream_bid(meta, DEFAULT_STREAM_ID, data);
    }
//	public void emit_bid(Object... values) throws InterruptedException {
//		emit_bid(DEFAULT_STREAM_ID, values);
//	}

    /**
     * @return the Task ids that received the tuples.
     * Currently spout and bolt emit are same... In the future, spout emit need to set up ACK..
     * TODO: 1) add streamId to support multiple stream feature
     * 2) add anchors (source/parent Brisk.execution.runtime.tuple) to support back-tracking for ack purpose.
     * @since 0.0.4 multiple stream ID added.
     * if left empty, use default stream Id instead.
     */
    public void emit(long bid, Object... values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, StreamValues values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, Object values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, deviceID, movingAvergeInstant, nextDouble);
    }

    public void emit(long bid, char[] key, long value) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, key, value);
    }

    public void force_emit(char[] values) throws InterruptedException {
        force_emit(DEFAULT_STREAM_ID, values);
    }

    public void emit(long bid, char[] values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(char[] values) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, values);
    }

    public void emit(char[] values, long bid, long timestamp) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, values, bid, timestamp);
    }

    /**
     * @param streamId
     * @param data
     * @return
     */
    private void emit_nowait(String streamId, Object... data) {
        if (executor.isLeafNode()) {
            return;
        }
        assert data != null && sc != null;
        sc.emitOnStream_nowait(meta, streamId, data);
    }

    private void emit_nowait(String streamId, char[] key, long value) {
        if (executor.isLeafNode()) {
            return;
        }
        assert key != null && sc != null;
        sc.emitOnStream_nowait(meta, streamId, key, value);
    }

    public void emit(String streamId, char[] key, long value) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        assert key != null && sc != null;
        sc.emitOnStream(meta, streamId, key, value);
    }

    public void emit(String streamId, char[] key, long value, long bid, long timestamp) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        assert key != null && sc != null;
        sc.emitOnStream(meta, streamId, key, value, bid, timestamp);
    }

    private void emit_nowait(String streamId, char[] str) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        assert str != null && sc != null;
        sc.emitOnStream_nowait(meta, streamId, str);
    }

    /**
     * @param values
     */
    public void emit_nowait(Object... values) {
        emit_nowait(DEFAULT_STREAM_ID, values);
    }

    public void emit_nowait(char[] key, long value) {
        emit_nowait(DEFAULT_STREAM_ID, key, value);
    }

    public void emit(char[] key, long value) throws InterruptedException {
        emit(DEFAULT_STREAM_ID, key, value);
    }

    public void emit_nowait(char[] str) throws InterruptedException {
        emit_nowait(DEFAULT_STREAM_ID, str);
    }

    public Marker emit_single(Object values) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, -1, values);
    }

    public Marker emit_single(String streamId, long bid, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data);
        return null;//marker
    }

    public Marker emit_single(String streamId, long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data);
        return null;//marker
    }

    public Marker emit_single(String streamId, long[] bid, long msg_id, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, msg_id, data);
        return null;//marker
    }

    public Marker emit_single(String streamId, long bid, Object data, long emit_time) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data, emit_time);
        return null;//marker
    }

    public Marker emit_single(String streamId, long bid, int p_id, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, data, p_id);
        return null;//marker
    }

    public Marker emit_single(String streamId, long[] bid, int p_id, long msg_id, int number_partitions, Object data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, msg_id, data, p_id, number_partitions);
        return null;//marker
    }

    public Marker emit_single(String streamId, long[] bid, int p_id, int number_partitions, Object data, long emit_timestamp) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, -1, data, p_id, number_partitions, emit_timestamp);//flag, pid, nump, emit-time.
        return null;//marker
    }

    public Marker emit_single(String streamId, long[] bid, int p_id, int number_partitions, Object data, long msg_id, long emit_timestamp) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, bid, msg_id, data, p_id, number_partitions, emit_timestamp);//flag, pid, nump, emit-time.
        return null;//marker
    }

    public Marker emit_single(long bid, char[] value) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, value);
    }

    public Marker emit_single(long bid, char[] value, long emit_time) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, emit_time, value);
    }

    public Marker emit_single(long bid, Set<Integer> keys) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, keys);
    }

    public Marker emit_single(long[] bid, long msg_id, Set<Integer> keys) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, msg_id, keys);
    }

    public Marker emit_single(long bid) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, true);
    }

    public Marker emit_single(long bid, long emit_time) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, emit_time);
    }

    public Marker emit_single(long bid, int[] signal) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, signal);
    }

    public Marker emit_single(long bid, boolean read_write, long emit_time) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, read_write, emit_time);
    }

    public Marker emit_single(long bid, int p_id, boolean read_write) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, p_id, read_write);
    }

    public Marker emit_single(long[] bid, int p_id, long msg_id, int number_partitions) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, p_id, msg_id, number_partitions, true);
    }

    public Marker emit_single(long[] bid, int p_id, long msg_id, int number_partitions, long emit_time) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, p_id, msg_id, number_partitions, emit_time);
    }

    public Marker emit_single(long[] bid, int p_id, int number_partitions, boolean read_write, long emit_timestamp) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, p_id, number_partitions, read_write, emit_timestamp);
    }

    public Marker emit_single(long[] bid, int p_id, int number_partitions, boolean read_write, long msg_id, long emit_timestamp) throws InterruptedException {
        return emit_single(DEFAULT_STREAM_ID, bid, p_id, number_partitions, read_write, msg_id, emit_timestamp);
    }

    public Marker emit_single(String streamId, long bid) throws InterruptedException {
        assert sc != null;
        sc.force_emitOnStream(meta, streamId, bid, bid);
        return null;//marker
    }

    public void broadcast_marker(long bid, Marker marker) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        sc.marker_boardcast(meta, bid, marker);
    }

    public void broadcast_marker(String streamId, long bid, Marker marker) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        sc.marker_boardcast(meta, streamId, bid, marker);
    }

    /**
     * Only ``sink" operator shall call this function!
     *
     * @param input
     * @param marker
     */
    public void ack(JumboTuple input, Marker marker) {
        assert this.executor.isLeafNode();
//		final int executorID = executor.getExecutorID();
//		//LOG.DEBUG(executor.getOP_full() + " is giving acknowledgement for marker:" + marker.msgId + " from " + input.getSourceTask());
//		final ExecutionNode src = input.getContext().getExecutor(input.getSourceTask());
//		src.op.callback(executorID, marker);
        //non-blocking ack.
        Runnable r = () -> {
            final int executorID = executor.getExecutorID();
//			//LOG.DEBUG(executor.getOP_full() + " is giving acknowledgement for marker:" + marker.msgId + " from " + input.getSourceTask());
            final ExecutionNode src = input.getContext().getExecutor(input.getSourceTask());
            src.op.callback(executorID, marker);
        };
        new Thread(r).start();
    }

    /**
     * @param input
     * @param marker
     */
    public void ack(Tuple input, Marker marker) {
        final int executorID = executor.getExecutorID();
        if (enable_debug)
            LOG.info(executor.getOP_full() + " is giving acknowledgement for marker:" + marker.msgId + " to " + input.getSourceComponent());
        final ExecutionNode src = input.getContext().getExecutor(input.getSourceTask());
        if (input.getBID() != totalEvents)
            src.op.callback(executorID, marker);
        //non-blocking ack.
//		Runnable r = () -> {
//			final int executorID = executor.getExecutorID();
//			//LOG.DEBUG(executor.getOP_full() + " is giving acknowledgement for marker:" + marker.msgId + " from " + input.getSourceTask());
//			final ExecutionNode src = input.getContext().getExecutor(input.getSourceTask());
//			src.op.callback(executorID, marker);
//		};
//
//		new Thread(r).start();
    }

    public void broadcast_ack(Marker marker) {
        final int executorID = this.executor.getExecutorID();
        for (TopologyComponent op : executor.getParents_keySet()) {
            for (ExecutionNode src : op.getExecutorList()) {
                src.op.callback(executorID, marker);
            }
        }
    }

    //	public void try_fill_gap(String streamId) {
//		sc.try_fill_gap(streamId);
//	}
//	public void try_fill_gap() {
//		try_fill_gap(DEFAULT_STREAM_ID);
//	}
    public long getBID(String streamId) {
        return sc.getBID(streamId);
    }

    public void create_marker_boardcast(long boardcast_time, long bid, int myiteration) throws InterruptedException {
        sc.create_marker_boardcast(meta, boardcast_time, bid, myiteration);
    }

    public void create_marker_boardcast(long boardcast_time, String streamId, long bid, int myiteration) throws InterruptedException {
        sc.create_marker_boardcast(meta, boardcast_time, streamId, bid, myiteration);
    }

    public void create_marker_single(long boardcast_time, String streamId, long bid, int myiteration) {
        sc.create_marker_single(meta, boardcast_time, streamId, bid, myiteration);
    }

//	public void increaseGap(String streamId) {
//		sc.increaseGap(streamId);
//	}
//	public void addGap(String streamId, long bid) {
//		sc.addGap(streamId, bid);
//	}
}
