package intellistream.morphstream.engine.stream.execution.runtime.collector;

import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.controller.output.OutputController;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.impl.Meta;
import intellistream.morphstream.engine.stream.execution.runtime.collector.impl.MetaGroup;
import intellistream.morphstream.util.OsUtils;
import intellistream.morphstream.util.datatypes.StreamValues;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

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

    public OutputCollector(ExecutionNode executor, TopologyContext context, int TotalEvents) {
        totalEvents = TotalEvents;
        int taskId = context.getThisTaskIndex();
        this.executor = executor;
        this.sc = executor.getController();
        this.meta = new MetaGroup(taskId);
        for (TopologyComponent childrenOP : this.executor.getChildren().keySet()) {
            this.meta.put(childrenOP, new Meta(taskId));
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
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
            forwardResult_inorder(Constants.DEFAULT_STREAM_ID, bid, gap, values);//package_Tuple(values, streamId)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void emit_inorder(long bid, LinkedList<Long> gap, char[] values) {
        try {
            forwardResult_inorder(Constants.DEFAULT_STREAM_ID, bid, gap, values);//package_Tuple(values, streamId)
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

    public void emit(String streamId, StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, streamId, -1, data);
    }

    public void emit(String data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, Constants.DEFAULT_STREAM_ID, -1, data);
    }

    public void emit(StreamValues data) throws InterruptedException {
        assert data != null && sc != null;
        sc.emitOnStream(meta, Constants.DEFAULT_STREAM_ID, -1, data);
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
        sc.force_emitOnStream(meta, Constants.DEFAULT_STREAM_ID, bid, data);
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
        sc.emitOnStream_bid(meta, Constants.DEFAULT_STREAM_ID, data);
    }
//	public void emit_bid(Object... values) throws InterruptedException {
//		emit_bid(DEFAULT_STREAM_ID, values);
//	}

    /**
     * @return the Task ids that received the tuples.
     * Currently spout and bolt emit are same... In the future, spout emit need to set up ACK..
     * TODO: 1) addOperation streamId to support multiple stream feature
     * 2) addOperation anchors (source/parent Brisk.execution.runtime.tuple) to support back-tracking for ack purpose.
     * @since 0.0.4 multiple stream ID added.
     * if left empty, use default stream Id instead.
     */
    public void emit(long bid, Object... values) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, StreamValues values) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, Object values) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, deviceID, movingAvergeInstant, nextDouble);
    }

    public void emit(long bid, char[] key, long value) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, key, value);
    }

    public void force_emit(char[] values) throws InterruptedException {
        force_emit(Constants.DEFAULT_STREAM_ID, values);
    }

    public void emit(long bid, char[] values) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, bid, values);
    }

    public void emit(char[] values) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, values);
    }

    public void emit(char[] values, long bid, long timestamp) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, values, bid, timestamp);
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

    /**
     * @param values
     */
    public void emit_nowait(Object... values) {
        emit_nowait(Constants.DEFAULT_STREAM_ID, values);
    }

    public void emit(char[] key, long value) throws InterruptedException {
        emit(Constants.DEFAULT_STREAM_ID, key, value);
    }

    public long getBID(String streamId) {
        return sc.getBID(streamId);
    }

}
