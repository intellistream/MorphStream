package intellistream.morphstream.examples.streaming.wordcount.util;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.base.MapBolt;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.examples.streaming.wordcount.util.WordCountConstants.Field;
import intellistream.morphstream.util.OsUtils;
import intellistream.morphstream.util.datatypes.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class WordCountBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
    private static final long serialVersionUID = -6454380680803776555L;

    private final Map<Integer, Long> counts = new HashMap<>();//what if memory is not enough to hold counts?

    public WordCountBolt() {
        super(LOG);
        this.setStateful();
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 80;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        char[] word = input.getCharArray(0);
        int key = Arrays.hashCode(word);
        long v = counts.getOrDefault(key, 0L);
        if (v == 0) {
            counts.put(key, 1L);
            collector.force_emit(0, new StreamValues(word, 1L));
        } else {
            long value = v + 1L;
            counts.put(key, value);
            collector.force_emit(0, new StreamValues(word, value));
        }
    }

    /**
     * MutableLong count = counts.computeIfAbsent(Arrays.hashCode(word), k -> new Long(0));
     * count.increment();
     *
     * @param input
     * @throws InterruptedException
     */
    @Override
    public void execute(JumboTuple input) throws InterruptedException {
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            String word = input.getString(0, i);
            int key = word.hashCode();
            counts.putIfAbsent(key, 0L);
            long count = counts.get(key) + 1;
            counts.put(key, count);
            StreamValues objects = new StreamValues(word, count);
            collector.emit(objects);
        }
    }

    public void display() {
        double size_state;
        size_state = counts.size();
        if (enable_log) LOG.info("Num of Tasks:" + this.getContext().getNUMTasks() + ", State size: " + size_state);
    }
}
