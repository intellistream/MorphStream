package application.bolts.comm;

import application.constants.BaseConstants;
import application.parser.SensorParser;
import application.helper.parser.Parser;
import application.util.Configuration;
import sesame.components.operators.base.MapBolt;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tony on 5/5/2017.
 * Use char[] to represent string!
 */
public class SensorParserBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SensorParserBolt.class);
    private static final long serialVersionUID = 7613878877612069900L;
    final SensorParser parser;
    private final Fields fields;

    public SensorParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (SensorParser) parser;
        this.fields = fields;
        this.read_selectivity = 2.0;
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 2.0);
    }

    public Integer default_scale(Configuration conf) {
//		int numNodes = conf.getInt("num_socket", 1);
        return 2;
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
            Object[] emit = parser.parse(string);
            for (int j = 0; j < 2; j++) {
                collector.emit(0, emit);
            }

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
            for (int j = 0; j < 10; j++) {
                Object[] emit = parser.parse(string);
                collector.emit_nowait(emit);
            }
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }
}
