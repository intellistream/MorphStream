package combo;

import benchmark.DataHolder;
import common.bolts.transactional.tp.*;
import common.collections.Configuration;
import common.datatype.AbstractLRBTuple;
import common.datatype.PositionReport;
import common.param.TxnEvent;
import common.param.lr.LREvent;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static content.Content.*;

public class TPCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(TPCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;
    HashMap<Integer, Integer> keys = new HashMap();
    DescriptiveStatistics stats = new DescriptiveStatistics();
    int concurrency = 0;
    int pre_concurrency = 0;
    int[] concerned_length = new int[]{1, 10, 50, 100, 250, 500, 750, 1000};
    int cnt = 0;
    ArrayDeque<LREvent> prevents = new ArrayDeque<>();

    public TPCombo() {
        super(LOG, 0);
    }

    private boolean check_conflict(LREvent pre_event, LREvent event) {
        Integer pre_segment = pre_event.getPOSReport().getSegment();
        Integer current_segment = event.getPOSReport().getSegment();
        return pre_segment.equals(current_segment);
    }

    private boolean conflict(LREvent event) {
        for (LREvent prevent : prevents) {
            if (check_conflict(prevent, event))
                return true;
        }
        return false;
    }

    protected void show_stats() {
        for (Object myevent : myevents) {
            Integer segment = ((LREvent) myevent).getPOSReport().getSegment();
            stats.addValue(segment);
            boolean containsKey = keys.containsKey(segment);
            if (containsKey) {
                keys.put(segment, keys.get(segment) + 1);
            } else {
                keys.put(segment, 1);
            }
        }
        for (Map.Entry<Integer, Integer> entry : keys.entrySet()) {
            LOG.info("SEGMENT:" + entry.getKey() + " " + "Counter:" + entry.getValue());
        }
        LOG.info(stats.toString());
        while (cnt < 8) {
            for (Object myevent : myevents) {
                if (!conflict((LREvent) myevent)) {
                    concurrency++;
                }
                prevents.add((LREvent) myevent);
                if (prevents.size() == concerned_length[cnt]) {
                    if (pre_concurrency == 0)
                        pre_concurrency = concurrency;
                    else
                        pre_concurrency = (pre_concurrency + concurrency) / 2;
                    concurrency = 0;
                    prevents.clear();
                }
            }
            System.out.println(concerned_length[cnt] + ",\t " + pre_concurrency * 2 + ",");
            cnt++;
        }
    }

    protected Object create_new_event(String record, int bid) {
        String[] token = record.split(" ");
        short type = Short.parseShort(token[0]);
        Short time = Short.parseShort(token[1]);
        Integer vid = Integer.parseInt(token[2]);
        if (type == AbstractLRBTuple.position_report) {
            return
                    new LREvent(new PositionReport(//
                            time,//
                            vid,//
                            Integer.parseInt(token[3]), // speed
                            Integer.parseInt(token[4]), // xway
                            Short.parseShort(token[5]), // lane
                            Short.parseShort(token[6]), // direction
                            Integer.parseInt(token[7]), // segment
                            Integer.parseInt(token[8])),
                            tthread,
                            bid)
                    ;
        } else {
            //ignore, not used in this experiment.
            return null;
        }
    }

    @Override
    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {
        int storageIndex = 0;
        //Load OnlineBiding Events.
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
        super.initialize(thread_Id, thisTaskId, graph);
        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        _combo_bid_size = combo_bid_size;
        switch (config.getInt("CCOption", 0)) {
            case CCOption_LOCK: {//no-order
                bolt = new TPBolt_nocc(0, sink);
                break;
            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new TPBolt_olb(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new TPBolt_lwm(0, sink);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new TPBolt_ts_s(0, sink);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new TPBolt_SSTORE(0, sink);
                _combo_bid_size = 1;
                break;
            }
        }
        //do preparation.
        bolt.prepare(config, context, collector);
        bolt.loadDB(config, context, collector);
        loadEvent(config.getString("rootFilePath"), config, context, collector);
        sink.checkpoint_interval = checkpoint_interval;
    }
}