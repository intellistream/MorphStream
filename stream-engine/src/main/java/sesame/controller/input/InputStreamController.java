package sesame.controller.input;

import sesame.execution.ExecutionNode;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.optimization.model.STAT;

import java.util.HashMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by shuhaozhang on 17/7/16.
 */
public abstract class InputStreamController implements IISC {
    private static final long serialVersionUID = 3675914097463516987L;
    /**
     * for an executor (except spout's executor), there's a receive queue
     * for *each* upstream executor's *each* stream output (if subscribed).
     * TODO:current design is restricted by ``partition parent operator" structure. This needs to be removed in future version.
     * streamId, SourceId -> queue
     *
     * @since 1.0, this restriction is removed because of multi-stream support.
     */
    private final HashMap<String, HashMap<Integer, Queue>> RQ = new HashMap<>();
    protected Set<String> keySet;
//    Deserializer _kryo;
TreeSet<JumboTuple> tuples = new TreeSet<>();//temporarily holds all retrieved tuples.

    protected InputStreamController() {
//        _kryo = new Deserializer();
    }

    public abstract JumboTuple fetchResults_inorder();

    public abstract Object fetchResults();

    public abstract Tuple fetchResults_single();

    public abstract JumboTuple fetchResults(STAT stat, int batch);

    /**
     * It needs to fetch from specific queue, which is costly, but not necessarily in production.
     *
     * @param src
     * @param stat
     * @param batch
     * @return
     */
    public JumboTuple fetchResults(ExecutionNode src, STAT stat, int batch) {
        //scanning through each receiving queue for data. one source may contain multiple streams.
//        JumboTuple[] t = new JumboTuple[batch];
        for (String streamId : keySet) {
//            int repeate = 1000;
            Queue queue = null;
//            stat.start_measure(repeate);
//            for (int r = 0; r < repeate; r++)
            queue = getRQ().get(streamId).get(src.getExecutorID());//~103 cycles
//            stat.end_measure();

            if (queue == null) {
                continue;
            }
//            for (int i = 0; i < batch; i++) {
//                t[i] = fetchFromqueue((P1C1Queue) queue, stat, batch);

            return fetchFromqueue(queue, stat, batch);
        }
        return null;
    }

    protected Tuple fetchFromqueue_single(Queue queue) {
        Tuple tuple;
        tuple = (Tuple) queue.poll();
        if (tuple != null) {
//            synchronized (queue) {
//                queue.notify();
//            }
            return tuple;
        }

        return null;
    }

    protected Object fetchFromqueue(Queue queue) {
        Object tuple;
        tuple = queue.poll();
        if (tuple != null) {
            synchronized (queue) {
                queue.notifyAll();
            }
            return tuple;
        }

        return null;
    }

    protected JumboTuple fetchFromqueue(Queue queue, STAT stat, int batch) {
        if (stat != null) {
            stat.start_measure();
        }
        JumboTuple tuple = (JumboTuple) fetchFromqueue(queue);
        if (tuple != null) {
            if (stat != null) {
                stat.end_measure_inFetch(batch);
            }
            return tuple;
        }
        return null;
    }

    protected JumboTuple fetchFromqueue_inorder(Queue queue) {

        if (!tuples.isEmpty()) {
            return tuples.pollFirst();
        }

        final int size = queue.size();//at this moment, how many elements are there?

        for (int i = 0; i < size; i++) {
            tuples.add((JumboTuple) fetchFromqueue(queue));
        }
        return tuples.pollFirst();
    }

//	/**
//	 * This is used for multi-consumer based implementation.
//	 * @param queue
//	 * @param stat
//	 * @param batch
//	 * @return
//	 */
//	protected JumboTuple fetchFromqueue(Queue queue) {
//		final JumboTuple tuple = fetch(queue);
//		if (tuple != null) {
//			if (queue instanceof SpmcArrayQueue || queue instanceof MpmcArrayQueue) {
//				final int targetTasks = tuple.getTargetId();
//				if (targetTasks != executor.getExecutorID()) {
//					queue.offer(tuple);
//					return null;
//				}
//			}
//		}
//		return tuple;
//	}


    public HashMap<String, HashMap<Integer, Queue>> getRQ() {
        return RQ;
    }

    public HashMap<Integer, Queue> getReceive_queue(String streamId) {
        return RQ.get(streamId);
    }

    public Set<String> getInputStreams() {
        return RQ.keySet();
    }

    public void initialize() {
        keySet = RQ.keySet();
    }

    /**
     * Should be called after executor initialize its own output queue.
     */
    public synchronized void setReceive_queue(String streamId, int executorID, Queue q) {
        HashMap<Integer, Queue> integerP1C1QueueHashMap = RQ.get(streamId);
        if (integerP1C1QueueHashMap == null) {
            integerP1C1QueueHashMap = new HashMap<>();
        }
        integerP1C1QueueHashMap.put(executorID, q);
        RQ.put(streamId, integerP1C1QueueHashMap);
    }


}
