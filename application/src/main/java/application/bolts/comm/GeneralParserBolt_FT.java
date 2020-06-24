package application.bolts.comm;

import application.helper.parser.Parser;
import sesame.components.operators.api.Checkpointable;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tony on 5/5/2017.
 */
public class GeneralParserBolt_FT<T> extends GeneralParserBolt implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(GeneralParserBolt_FT.class);
    private static final long serialVersionUID = -1767315135506219412L;


    public GeneralParserBolt_FT(Parser parser, Fields fields) {
        super(parser, fields);
        state = new ValueState();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

            final Marker marker = in.getMarker(i);
            if (marker != null) {
                forward_checkpoint(in.getSourceTask(), bid, marker);
                continue;
            }

            char[] string = in.getCharArray(0, i);
            Object emit = parser.parse(string);
            collector.emit(bid, emit);

        }
    }

    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        final boolean check = checkpoint_forward(sourceId);//simply forward marker when it is ready.
        if (check) {
            this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
            //LOG.DEBUG(this.getContext().getThisComponentId() + this.getContext().getThisTaskId() + " broadcast marker with id:" + marker.msgId + "@" + DateTime.now());
        }
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) {

    }


    @Override
    public void ack_checkpoint(Marker marker) {

        //Do something to clear past state. (optional)

        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}
