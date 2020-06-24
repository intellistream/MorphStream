package application.bolts.wc;

import application.constants.BaseConstants;
import application.constants.WordCountConstants.Field;
import application.util.Configuration;
import application.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.operators.base.splitBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;

import java.util.HashMap;

//import static Brisk.state_engine.utils.Utils.printAddresses;

public class SplitSentenceBolt extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private static final long serialVersionUID = 8089145995668583749L;
//	long end = 0;
//	boolean GetAndUpdate = false;
//	int loop = 1;
//	private int curr = 0, precurr = 0;
//	private int dummy = 0;

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
//		LOG.info("PID  = " + pid);
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//		String value_list = in.getString(0);
//		String[] split = value_list.split(",");
//		for (String word : split) {
//			collector.force_emit(word);
//		}


        char[] value = in.getCharArray(0);
        int index = 0;
        int length = value.length;
        for (int c = 0; c < length; c++) {
            if (value[c] == ',' || c == length - 1) {//double measure_end.
                int len = c - index;
                char[] word = new char[len];
                System.arraycopy(value, index, word, 0, len);
                collector.force_emit(word);
                index = c + 1;
            }
        }
    }

    public void execute(JumboTuple in) throws InterruptedException {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

//			char[] value_list = in.getCharArray(0, i);
//			int index = 0;
//			int length = value_list.length;
//			for (int c = 0; c < length; c++) {
//				if (value_list[c] == ',' || c == length - 1) {//double measure_end.
//					int len = c - index;
//					char[] word = new char[len];
//					System.arraycopy(value_list, index, word, 0, len);
//					collector.emit(word);
//					index = c + 1;
//				}
//			}

            char[] value = in.getCharArray(0, i);
            String[] split = new String(value).split(",");
            for (String word : split) {
                collector.emit(-1, word.toCharArray());
            }
        }
    }


    public void profile_execute(JumboTuple in) throws InterruptedException {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

//			char[] value_list = in.getCharArray(0, i);
//			int index = 0;
//			int length = value_list.length;
//			for (int c = 0; c < length; c++) {
//				if (value_list[c] == ',' || c == length - 1) {//double measure_end.
//					int len = c - index;
//					char[] word = new char[len];
//					System.arraycopy(value_list, index, word, 0, len);
//					collector.emit_nowait(word);
//					index = c + 1;
//				}
//			}

            char[] value = in.getCharArray(0, i);
            String[] split = new String(value).split(",");
            for (String word : split) {
                collector.emit_nowait(word.toCharArray());
            }


        }
    }
}
