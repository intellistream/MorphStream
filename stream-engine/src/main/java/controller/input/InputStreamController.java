package controller.input;

import execution.runtime.tuple.JumboTuple;

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
    TreeSet<JumboTuple> tuples = new TreeSet<>();//temporarily holds all retrieved tuples.

    protected InputStreamController() {
    }

    public abstract Object fetchResults();

    public abstract Object fetchResultsIndex(int index);

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

    //Fetch tuple with matching bid
    protected Object fetchFromQueue(Queue queue, int index) {
        Object peekTuple;
        Object tuple;
        peekTuple = queue.peek();
        if (peekTuple != null) {
            synchronized (queue) {
                int bid = (int) ((JumboTuple) peekTuple).getBID();
                if ((bid + 1) % index == 0) { //increase bid to make it start from 1, avoid zero division
                    tuple = queue.poll();
                    queue.notifyAll();
                    return tuple;
                }
            }
        }
        return null;
    }

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
