package sesame.faulttolerance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.tuple.impl.Marker;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import static application.CONTROL.enable_debug;

/**
 * @param <E> actual contents
 */
public abstract class State<E extends Serializable> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(State.class);
    private static final long serialVersionUID = 2L;
    public transient Writer writer;
    private transient HashMap<Integer, Boolean> source_ready;
    private transient HashMap<Integer, Boolean> consumer_ack;

    public State() {

    }

    public abstract void update(E value);

    public abstract E value();

    public abstract void clean();

    /**
     * have received all ack from consumers.
     *
     * @return
     */
    boolean all_dst_ack() {
        return !(consumer_ack.containsValue(false));
    }

    public void source_state_ini(ExecutionNode executor) {
        if (source_ready == null) {
            source_ready = new HashMap<>();
            for (TopologyComponent op : executor.getParents_keySet()) {
                for (ExecutionNode src : op.getExecutorList()) {
                    source_ready.put(src.getExecutorID(), false);
                }
            }
        } else {
            for (Integer integer : source_ready.keySet()) {
                source_ready.put(integer, false);
            }
        }
    }

    public void dst_state_init(ExecutionNode executor) {
        if (consumer_ack == null) {
            consumer_ack = new HashMap<>();
            for (TopologyComponent op : executor.getChildren_keySet()) {
                for (ExecutionNode dst : op.getExecutorList()) {
                    consumer_ack.put(dst.getExecutorID(), false);
                }
            }

        } else {
            for (Integer integer : consumer_ack.keySet()) {
                consumer_ack.put(integer, false);
            }

        }
    }

    /**
     * have received all tuples from source.
     *
     * @return
     */
    protected boolean all_src_ready() {

        return !(source_ready.containsValue(false));
    }

    public boolean forward(int sourceId, ExecutionNode executor) {
        source_ready.put(sourceId, true);
        if (all_src_ready()) {
            source_state_ini(executor);
            return true;
        }
        return false;//not ready yet, do not forward the marker.

    }

    /**
     * 4. With compress and shared.
     *
     * @param value
     * @param sourceId
     * @param marker
     * @param executor
     * @param path
     * @return
     */
    public boolean share_store(E value, int sourceId, Marker marker, ExecutionNode executor, String path) {
        source_ready.put(sourceId, true);
        if (all_src_ready()) {
            try {
                if (value != null) {
                    update(value);
                    writer.save_state_MMIO_shared(marker.msgId, marker.timeStampNano, marker.getMyiteration()
                            , path, executor, this);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            source_state_ini(executor);
            return true;
        }
        return false;//not ready yet, do not forward the marker.
    }


    /**
     * 3. With compress.
     *
     * @param value
     * @param sourceId
     * @param marker
     * @param executor
     * @param path
     * @return
     */
    public boolean compress_store(E value, int sourceId, Marker marker, ExecutionNode executor, String path) {
        source_ready.put(sourceId, true);
        if (all_src_ready()) {
            try {
                if (value != null) {
                    update(value);
                    writer.save_state_MMIO_compress(marker.msgId, marker.timeStampNano, marker.getMyiteration()
                            , path, true, executor, this);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            source_state_ini(executor);
            return true;
        }
        return false;//not ready yet, do not forward the marker.
    }

    /**
     * 2. No compress.
     *
     * @param value
     * @param sourceId
     * @param marker
     * @param executor
     * @param path
     * @return
     */
    public boolean store(E value, int sourceId, Marker marker, ExecutionNode executor, String path) {
        source_ready.put(sourceId, true);
        if (all_src_ready()) {
            try {
                if (value != null) {
                    update(value);
                    writer.save_state_MMIO_compress(marker.msgId, marker.timeStampNano, marker.getMyiteration()
                            , path, false, executor, this);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            source_state_ini(executor);
            return true;
        }
        return false;//not ready yet, do not forward the marker.
    }

    /**
     * 1. Native.
     *
     * @param value
     * @param sourceId
     * @param marker
     * @param executor
     * @param path
     * @return
     */
    public boolean native_store(E value, int sourceId, Marker marker, ExecutionNode executor, String path) {
        source_ready.put(sourceId, true);
        if (all_src_ready()) {
            try {
                if (value != null) {
                    update(value);
                    writer.save_state(marker.msgId, marker.timeStampNano, marker.getMyiteration(), path, executor, this);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            source_state_ini(executor);
            return true;
        }
        return false;//not ready yet, do not forward the marker.
    }


    public synchronized void callback_bolt(int callee, Marker marker, ExecutionNode executor) {
        consumer_ack.put(callee, true);
        if (all_dst_ack()) {
            LOG.trace(executor.getOP_full() + " received ack from all consumers.");
            //	writer.save_state_MMIO_synchronize(executor); // enable if fault-tolerance enabled.
            dst_state_init(executor);//reset state.
            executor.clean_state(marker);
        }
    }

    public synchronized void callback_spout(int callee, Marker marker, ExecutionNode executor) {
        consumer_ack.put(callee, true);
//        executor.earlier_clean_state(marker);
        if (all_dst_ack()) {
            if (enable_debug)
                LOG.info(executor.getOP_full() + " received ack from all consumers.");
            dst_state_init(executor);
            executor.clean_state(marker);
        }
    }


}
