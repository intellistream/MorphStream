package intellistream.morphstream.engine.stream.controller.output;

import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.components.MultiStreamComponent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.streaminfo;
import intellistream.morphstream.engine.stream.execution.runtime.collector.impl.MetaGroup;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.util.datatypes.StreamValues;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by shuhaozhang on 15/7/16.
 * Every executor has a outputStream Controller.
 */
public class MultiStreamOutputContoller extends OutputController {
    private static final long serialVersionUID = 4083107089944803247L;
    /**
     * multiple stream map. < StreamId, DownOpId, PC >
     */
    private final HashMap<String, HashMap<String, PartitionController>> PClist;
    private final HashMap<String, PartitionController[]> collections;
    private final HashMap<String, Integer> counter;

    public MultiStreamOutputContoller(
            MultiStreamComponent op,
            HashMap<String, HashMap<String, PartitionController>> PClist) {
        super();
        HashMap<String, streaminfo> output_streams = op.getOutput_streams();
        this.PClist = PClist;
        collections = new HashMap<>();
        counter = new HashMap<>();//to fight against GC.
        for (String streamId : output_streams.keySet()) {
            PartitionController[] PartitionControllers = new PartitionController[getPartitionController(streamId).size()];
            for (int i = 0; i < getPartitionController(streamId).size(); i++) {
                PartitionControllers[i] = getPartitionController(streamId).iterator().next();
            }
            counter.put(streamId, 0);
            collections.put(streamId, PartitionControllers);
        }
    }

    @Override
    public PartitionController getPartitionController(String streamId, String boltID) {
        return PClist.get(streamId).get(boltID);
    }

    @Override
    public Collection<PartitionController> getPartitionController() {
        return PClist.get(Constants.DEFAULT_STREAM_ID).values();
    }

    @Override
    public Collection<PartitionController> getPartitionController(String StreamId) {
        return PClist.get(StreamId).values();
    }

    public boolean isEmpty() {
        for (String stream : PClist.keySet()) {
            for (String op : PClist.get(stream).keySet()) {
                if (!PClist.get(stream).get(op).isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void allocatequeue(boolean linked, int desired_elements_epoch_per_core) {
        for (String stream : PClist.keySet()) {
            for (String op : PClist.get(stream).keySet()) {
                PClist.get(stream).get(op).allocate_queue(linked, desired_elements_epoch_per_core);
            }
        }
    }

    /**
     * Broadcast output tuples to all downstream operators.
     *
     * @param MetaGroup
     * @param streamId
     * @param bid
     * @param output
     * @return
     */
    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, output);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, output);
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, bid, data);
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long bid, Object... data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, bid, data);
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, long[] bid, long msg_id, Object... data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, bid, msg_id, data);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, StreamValues data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, data);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, int deviceID, double nextDouble, double movingAvergeInstant) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, deviceID, movingAvergeInstant, nextDouble);
        }
    }

    @Override
    public void force_emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.force_emit(MetaGroup.get(p.childOP), streamId, -1, data);//default bid is -1
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, -1, data);//default bid is -1
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] data, long bid, long timestamp) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, -1, data, bid, timestamp);//default bid is -1
        }
    }

    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, output);
        }
    }

    public void emitOnStream(MetaGroup MetaGroup, String streamId, long bid, char[] key, long value) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, bid, key, value);
        }
    }

    /**
     * Broadcast output tuples to all downstream operators.
     *
     * @param MetaGroup
     * @param streamId
     * @param bid
     * @param gap
     * @param output
     * @return
     */
    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, Object... output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_inorder(MetaGroup.get(p.childOP), streamId, bid, gap, output);
        }
    }

    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, char[] output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_inorder(MetaGroup.get(p.childOP), streamId, bid, gap, output);
        }
    }

    @Override
    public void emitOnStream_inorder(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_inorder(MetaGroup.get(p.childOP), streamId, bid, gap, tuple);
        }
    }

    @Override
    public void emitOnStream_inorder_single(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap, StreamValues tuple) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_inorder_single(MetaGroup.get(p.childOP), streamId, bid, gap, tuple);
        }
    }

    @Override
    public void emitOnStream_inorder_push(MetaGroup MetaGroup, String streamId, long bid, LinkedList<Long> gap) {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_inorder_push(MetaGroup.get(p.childOP), streamId, bid, gap);
        }
    }

    /**
     * Broadcast output tuples to all downstream operators.
     *
     * @param MetaGroup
     * @param streamId
     * @param output
     * @return
     */
    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object... output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_bid(MetaGroup.get(p.childOP), streamId, output);
        }
    }

    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, Object output) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_bid(MetaGroup.get(p.childOP), streamId, output);
        }
    }

    @Override
    public void emitOnStream_bid(MetaGroup MetaGroup, String streamId, char[] data) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_bid(MetaGroup.get(p.childOP), streamId, data);
        }
    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, Object... output) {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_nowait(MetaGroup.get(p.childOP), streamId, output);
        }
    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] key, long value) {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_nowait(MetaGroup.get(p.childOP), streamId, key, value);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, key, value);
        }
    }

    @Override
    public void emitOnStream(MetaGroup MetaGroup, String streamId, char[] key, long value, long bid, long TimeStamp) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit(MetaGroup.get(p.childOP), streamId, key, value, bid, TimeStamp);
        }
    }

    @Override
    public void emitOnStream_nowait(MetaGroup MetaGroup, String streamId, char[] str) throws InterruptedException {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.emit_nowait(MetaGroup.get(p.childOP), streamId, str);
        }
    }

    @Override
    public void create_marker_boardcast(MetaGroup meta, long timestamp, long bid, int myiteration) {
        //non-blocking boardcast.
//		Runnable r = () -> {
//			for (PartitionController p :
//					PClist.GetAndUpdate(streamId).values()) {
//				p.create_marker_boardcast(MetaGroup.GetAndUpdate(p.childOP), streamId, timestamp, msgID, myiteration);
//			}
//		};
//		new Thread(r).start();
        for (String streamId : PClist.keySet()) {
            PartitionController[] it = collections.get(streamId);
            for (int i = 0; i < it.length; i++) {
                PartitionController p = it[i];
                p.create_marker_boardcast(meta.get(p.childOP), streamId, timestamp, bid, myiteration);
            }
        }
    }

    @Override
    public void create_marker_single(MetaGroup meta, long timestamp, String streamId, long bid, int myiteration) {
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.create_marker_single(meta.get(p.childOP), streamId, timestamp, bid, myiteration);
        }
    }

    @Override
    public void create_marker_boardcast(MetaGroup meta, long timestamp, String streamId, long bid, int myiteration) {
//        non-blocking boardcast.
//		Runnable r = () -> {
//			for (PartitionController p :
//					PClist.GetAndUpdate(streamId).values()) {
//                try {
//                    p.create_marker_boardcast(common.meta.GetAndUpdate(p.childOP), streamId, timestamp, bid, myiteration);
//                } catch (InterruptedException e) {
////                    e.printStackTrace();
//                }
//            }
//		};
//		new Thread(r).start();
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.create_marker_boardcast(meta.get(p.childOP), streamId, timestamp, bid, myiteration);
        }
    }

    @Override
    public void marker_boardcast(MetaGroup MetaGroup, long bid, Marker marker) {
//		//non-blocking boardcast.
//		Runnable r = () -> {
//			for (PartitionController p :
//					PClist.GetAndUpdate(streamId).values()) {
//				p.marker_boardcast(MetaGroup.GetAndUpdate(p.childOP), streamId, marker);
//			}
//		};
//		new Thread(r).start();
        for (String streamId : PClist.keySet()) {
            PartitionController[] it = collections.get(streamId);
            for (int i = 0; i < it.length; i++) {
                PartitionController p = it[i];
                p.marker_boardcast(MetaGroup.get(p.childOP), streamId, bid, marker);
            }
        }
    }

    @Override
    public void marker_boardcast(MetaGroup MetaGroup, String streamId, long bid, Marker marker) {
//		//non-blocking boardcast.
//		Runnable r = () -> {
//			for (PartitionController p :
//					PClist.GetAndUpdate(streamId).values()) {
//				p.marker_boardcast(MetaGroup.GetAndUpdate(p.childOP), streamId, marker);
//			}
//		};
//		new Thread(r).start();
        PartitionController[] it = collections.get(streamId);
        for (int i = 0; i < it.length; i++) {
            PartitionController p = it[i];
            p.marker_boardcast(MetaGroup.get(p.childOP), streamId, bid, marker);
        }
    }

    @Override
    public void setContext(int executorID, TopologyContext context) {
        for (String stream : PClist.keySet()) {
            for (String op : PClist.get(stream).keySet()) {
                PClist.get(stream).get(op).setContext(executorID, context);
            }
        }
    }

    /**
     * TODO: not sure if this is still needed.
     *
     * @param streamId
     * @return
     */
    public long getBID(String streamId) {
        return 0;
//		return PClist.GetAndUpdate(streamId).values().iterator().next().getBID();
    }
}
