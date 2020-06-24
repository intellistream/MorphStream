package application.bolts.comm;

import application.parser.StringParser;
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
public class StringParserBolt_latency extends MapBolt {
    private static final long serialVersionUID = 7613878877612069900L;
    private static final Logger LOG = LoggerFactory.getLogger(StringParserBolt_latency.class);
    final StringParser parser;
    private final Fields fields;

    public StringParserBolt_latency(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (StringParser) parser;
        this.fields = fields;
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
            char[] emit = parser.parse(string);

            long msgId = in.getLong(1, i);
//			if (msgId != -1) {
            collector.emit_nowait(emit, msgId, in.getLong(2, i));
//			} else {
//				collector.emit(emit, msgId, 0);
//			}
        }
    }

    @Override
    public void profile_execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
            char[] emit = parser.parse(string);
            collector.emit_nowait(emit);
        }
    }
}
