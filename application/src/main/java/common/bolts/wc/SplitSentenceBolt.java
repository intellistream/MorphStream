package common.bolts.wc;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import common.constants.WordCountConstants.Field;
import components.operators.base.splitBolt;
import execution.ExecutionGraph;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

//import static Brisk.utils.Utils.printAddresses;
public class SplitSentenceBolt extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private static final long serialVersionUID = 8089145995668583749L;
    String regex = "[\\s,]+";

    public SplitSentenceBolt() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 10;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID();
//		if (enable_log) LOG.info("PID  = " + pid);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        String value = in.getString(0);
        String[] split = value.split(regex);
        for (String word : split) {
            collector.emit(0, word);
        }
    }

    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            String value = in.getString(0, i);
            if (value != null) {
                String[] split = value.split(regex);
                for (String word : split) {
                    collector.emit(0, word);
                }
            }
        }
    }
}
