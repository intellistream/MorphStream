package intellistream.morphstream.engine.stream.components.operators.api.delete;

import intellistream.morphstream.engine.stream.components.operators.api.Operator;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * Abstract AbstractSpout is a special partition-pass Operator.
 */
public abstract class AbstractSpout extends Operator {
    private static final long serialVersionUID = -7455539617930687503L;
    //the following are used for checkpoint
    protected int myiteration = 0;//start from 1st iteration.
    protected boolean success = true;
    protected long boardcast_time;
    protected ArrayList<String> array;
    protected int counter = 0;
    protected int taskId;
    protected int cnt;

    protected AbstractSpout(Logger log) {
        super(log, 1);
    }

    public abstract void nextTuple() throws InterruptedException;

}
