package application.bolts.comm;

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
 */
public class GeneralParserBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeneralParserBolt.class);
    private static final long serialVersionUID = 2921418272215488770L;
    final Parser parser;
    private final Fields fields;

    public GeneralParserBolt(Parser parser, Fields fields) {
        super(LOG);
        this.parser = parser;
        this.fields = fields;
    }

    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 6;
        } else {
            return 2;
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

//	volatile Object[] items;
//	Object[] items;

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] string = in.getCharArray(0, i);
            Object emit = parser.parse(string);
            collector.emit(bid, emit);
        }
//		//LOG.DEBUG("Parser(" + this.getContext().getThisTaskId() + ") emit:" + bid);
//		this.collector.try_fill_gap();
    }
}
