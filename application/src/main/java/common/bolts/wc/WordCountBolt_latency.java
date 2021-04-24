package common.bolts.wc;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.operators.base.MapBolt;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static common.constants.BaseConstants.BaseField.MSG_ID;
import static common.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
public class WordCountBolt_latency extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_latency.class);
    private static final long serialVersionUID = -6454380680803776555L;
    //private int total_thread=context.getThisTaskId();
//    private static final String splitregex = " ";
//    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<Integer, Long> counts = new HashMap<>();
    public WordCountBolt_latency() {
        super(LOG);
        this.setStateful();
    }
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return numNodes;
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID();
//		LOG.info("PID  = " + pid);
    }
    @Override
    public Fields getDefaultFields() {
        return new Fields(MSG_ID, SYSTEMTIMESTAMP, Field.WORD, Field.COUNT);
    }
    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use.
    }
    //a workaround to de-cache, otherwise, we have to profile Cpro under varying replication setting.
    /*volatile String word;*/
    /*volatile MutableLong count;*/
    /**
     * MutableLong count = counts.computeIfAbsent(Arrays.hashCode(word), k -> new Long(0));
     * count.increment();
     *
     * @param in
     * @throws InterruptedException
     */
    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
//		final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            long msgID = in.getLong(1, i);
            long TimeStamp;
//			if (msgID != -1) {
            TimeStamp = in.getLong(2, i);
//			} else {
//				TimeStamp = 0;
//			}
            char[] word = in.getCharArray(0, i);
            //Long value_list = counts.putIfAbsent(Arrays.hashCode(word), 1L);
//			StreamValues value_list = new StreamValues(word, count.longValue());
            int key = Arrays.hashCode(word);
            long v = counts.getOrDefault(key, 0L);
            if (v == 0) {
                counts.put(key, 1L);
                collector.emit_nowait(msgID, TimeStamp, word, 1L);
            } else {
                long value = v + 1L;
                counts.put(key, value);
                collector.emit_nowait(msgID, TimeStamp, word, value);
            }
        }
    }
    public void display() {
        double size_state;
//		if (OsUtils.isUnix()) {
//			size_state = MemoryUtil.deepMemoryUsageOf(counts, MemoryUtil.VisibilityFilter.ALL);
//		} else {
        size_state = counts.size();
//		}
//
        LOG.info("Num of Tasks:" + this.getContext().getNUMTasks() + ", State size: " + size_state);
//		for (Map.Entry<String, MutableLong> entry : counts.entrySet()) {
//			System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//		}
    }
}
