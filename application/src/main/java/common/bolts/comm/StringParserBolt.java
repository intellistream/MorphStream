package common.bolts.comm;

import common.collections.Configuration;
import common.helper.parser.Parser;
import common.parser.StringParser;
import components.operators.base.MapBolt;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Fields;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tony on 5/5/2017.
 * Use char[] to represent string!
 */
public class StringParserBolt extends MapBolt {
    private static final long serialVersionUID = 7613878877612069900L;
    private static final Logger LOG = LoggerFactory.getLogger(StringParserBolt.class);
    final StringParser parser;
    private final Fields fields;

    public StringParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = (StringParser) parser;
        this.fields = fields;
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 4;
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
        String string = in.getString(0);
        String emit = parser.parse(string);
        collector.force_emit(emit);
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            String string = in.getString(0, i);
            String streamValues = parser.parse(string);
            collector.emit(0, streamValues);
        }
    }
}
