package application.bolts.comm;

import application.parser.TransactionParser;
import application.helper.parser.Parser;
import application.util.Configuration;
import sesame.components.operators.base.MapBolt;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by tony on 5/5/2017.
 * Use char[] to represent string!
 */
public class FraudDetectionParserBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionParserBolt.class);
    private static final long serialVersionUID = 7613878877612069900L;
    final TransactionParser parser;
    private final Fields fields;

    public FraudDetectionParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (TransactionParser) parser;
        this.fields = fields;
        this.read_selectivity = 3;
    }

    @Override
    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 21;
        } else {
            return 1;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return fields;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
//			LOG.info(String.valueOf(string.length));
            Object[] emit = parser.parse(string);
            collector.emit(0, emit);
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }

    @Override
    public void profile_execute(JumboTuple in) {
//		final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
//			LOG.info(String.valueOf(string.length));
            Object[] emit = parser.parse(Arrays.copyOf(string, string.length));
            collector.emit_nowait(emit);
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }
}
