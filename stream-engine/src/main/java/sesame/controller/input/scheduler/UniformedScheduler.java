package sesame.controller.input.scheduler;

import application.util.CompactHashMap.QuickHashMap;
import sesame.controller.input.InputStreamController;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.optimization.model.STAT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * TODO: The over head is still too large.
 * Created by shuhaozhang on 17/7/16.
 */
public class UniformedScheduler extends InputStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(UniformedScheduler.class);
    private static final long serialVersionUID = -8233684569637244620L;
    private final QuickHashMap<String, Integer[]> queues = new QuickHashMap<>();
    private String[] streams;
    private int stream_index = 0;
    private int queue_index = 0;

    public void initialize() {
        super.initialize();
        this.streams = Arrays.copyOf(getRQ().keySet().toArray(), getRQ().keySet().size(), String[].class);
        for (String streamId : getRQ().keySet()) {
            queues.put(streamId, Arrays.copyOf(getRQ().get(streamId).keySet().toArray(), getRQ().get(streamId).size(), Integer[].class));
        }
    }


    @Override
    public JumboTuple fetchResults_inorder() {
        //        JumboTuple[] t = new JumboTuple[batch];
        for (int i = stream_index++; i < streams.length + stream_index; i++) {
            String streamId = streams[i % streams.length];
            //assert RQ != null;
            //final HashMap<Integer, P1C1Queue<JumboTuple>> integerP1C1QueueHashMap = RQ.GetAndUpdate(streamId);
            Integer[] qids = queues.get(streamId);
            int queueIdLength = qids.length;
            for (int j = queue_index++; j < queueIdLength + queue_index; ) {
                //Integer[] queueId = Arrays.copyOf(integerP1C1QueueHashMap.keySet().toArray(),integerP1C1QueueHashMap.fieldSize(),Integer[].class);
                int q_index = qids[j % queueIdLength];
                if (queueIdLength > 1)
                    //LOG.DEBUG("Uniformed shoulder, queue index:" + q_index);

//                for (int b = 0; b < batch; b++) {
//                    t[b] = fetchFromqueue((P1C1Queue) getRQ().GetAndUpdate(streamId).GetAndUpdate(q_index));
//                }
                return fetchFromqueue_inorder(getRQ().get(streamId).get(q_index));
            }
        }
        return null;
    }

    @Override
    public Object fetchResults() {
//        JumboTuple[] t = new JumboTuple[batch];
        for (int i = stream_index++; i < streams.length + stream_index; i++) {
            String streamId = streams[i % streams.length];
            //assert RQ != null;
            //final HashMap<Integer, P1C1Queue<JumboTuple>> integerP1C1QueueHashMap = RQ.GetAndUpdate(streamId);
            Integer[] qids = queues.get(streamId);
            int queueIdLength = qids.length;
            for (int j = queue_index++; j < queueIdLength + queue_index; ) {
                //Integer[] queueId = Arrays.copyOf(integerP1C1QueueHashMap.keySet().toArray(),integerP1C1QueueHashMap.fieldSize(),Integer[].class);
                int q_index = qids[j % queueIdLength];
                if (queueIdLength > 1)
                    //LOG.DEBUG("Uniformed shoulder, queue index:" + q_index);

//                for (int b = 0; b < batch; b++) {
//                    t[b] = fetchFromqueue((P1C1Queue) getRQ().GetAndUpdate(streamId).GetAndUpdate(q_index));
//                }
                return fetchFromqueue(getRQ().get(streamId).get(q_index));
            }
        }
        return null;
    }

    @Override
    public Tuple fetchResults_single() {

        throw new UnsupportedOperationException();
    }

    @Override
    public JumboTuple fetchResults(STAT stat, int batch) {
        return null;
    }
}
