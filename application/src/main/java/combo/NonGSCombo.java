package combo;

import benchmark.DataHolder;
import common.bolts.transactional.nongs.NonGSBolt_sstore;
import common.bolts.transactional.nongs.NonGSBolt_ts;
import common.collections.Configuration;
import engine.txn.TxnEvent;
import common.param.mb.MicroEvent;
import engine.stream.components.context.TopologyContext;
import engine.stream.execution.ExecutionGraph;
import engine.stream.execution.runtime.collector.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

import static common.CONTROL.*;
import static engine.txn.content.Content.*;

public class NonGSCombo extends SPOUTCombo{
    private static final Logger LOG = LoggerFactory.getLogger(NonGSCombo.class);
    private static final long serialVersionUID = -2156693261209711017L;
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{40};
    int cnt = 0;
    ArrayDeque<MicroEvent> prevents = new ArrayDeque<>();

    public NonGSCombo() {
        super(LOG, 0);
    }

    @Override
    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector){
        int storageIndex = 0;
        //Load NonMicro Events.
        for (int index = taskId; index < DataHolder.events.size(); ) {
            TxnEvent event = DataHolder.events.get(index).cloneEvent();
            mybids[storageIndex] = event.getBid();
            myevents[storageIndex++] = event;
            if (storageIndex == num_events_per_thread)
                break;
            index += tthread * combo_bid_size;
        }
        assert (storageIndex == num_events_per_thread);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        assert enable_shared_state;//This application requires enable_shared_state.

        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_TStream: {//T-Stream
                bolt = new NonGSBolt_ts(0, sink);
                break;
            }
            case CCOption_SStore: {//S-Store
                bolt = new NonGSBolt_sstore(0, sink);
                break;
            }
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        bolt.loadDB(config, context, collector);
        loadEvent(config.getString("rootFilePath"), config, context, collector);
    }
}