package intellistream.morphstream.api.operator.spout;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleConf;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;


public class ApplicationSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpout.class);
    private RdmaShuffleManager rdmaShuffleManager;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpout() throws Exception {
        super(LOG, 0);
        if (conf.getBoolean("isRemote", false)) {
            this.rdmaShuffleManager = new RdmaShuffleManager(new RdmaShuffleConf(conf), conf.getBoolean("isDriver"));
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {
        if (!inputQueue.isEmpty()) {
            TransactionalEvent event = inputQueue.take(); //this should be txnEvent already
            long bid = event.getBid();

            if (bid != -1) { //txn events
                if (CONTROL.enable_latency_measurement)
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
                else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                collector.emit(bid, tuple); // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                    if (model_switch(counter)) {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "punctuation"));
                        collector.emit(bid, marker);
                    }
                }
            } else { //control signals
                if (Objects.equals(event.getFlag(), "pause")) {
                    marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "pause"));
                    collector.emit(bid, marker);
                }
            }

            if (inputQueue.isEmpty()) { //TODO: Refactor this part, remove the_end, use stopEvent indicator
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                getContext().stop_running();
            }
        }
    }
}
