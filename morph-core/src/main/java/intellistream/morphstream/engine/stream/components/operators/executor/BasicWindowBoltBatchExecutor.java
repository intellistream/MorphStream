package intellistream.morphstream.engine.stream.components.operators.executor;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.AbstractWindowedBolt;
import intellistream.morphstream.engine.stream.components.operators.api.BaseWindowedBolt;
import intellistream.morphstream.engine.stream.components.windowing.*;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class BasicWindowBoltBatchExecutor extends BoltExecutor {
    private static final long serialVersionUID = 369470953543118848L;
    AbstractWindowedBolt _op;
    private transient WindowManager<Tuple> windowManager;
    private transient BaseWindowedBolt.Duration windowLengthDuration;

    public BasicWindowBoltBatchExecutor(AbstractWindowedBolt op) {
        super(op);
        _op = op;
    }

    private WindowManager<Tuple> initWindowManager(WindowLifecycleListener<Tuple> lifecycleListener, Map<String, Object> topoConf,
                                                   TopologyContext context, Collection<Event<Tuple>> queue) {
        WindowManager<Tuple> manager = new WindowManager<>(lifecycleListener, queue);
        BaseWindowedBolt.Count windowLengthCount = null;
        BaseWindowedBolt.Duration slidingIntervalDuration = null;
        BaseWindowedBolt.Count slidingIntervalCount = null;
        // window length
        if (topoConf.containsKey(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)) {
            windowLengthCount = new BaseWindowedBolt.Count(((Number) topoConf.get(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT)).intValue());
        } else if (topoConf.containsKey(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)) {
            windowLengthDuration = new BaseWindowedBolt.Duration(
                    ((Number) topoConf.get(Configuration.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS)).intValue(),
                    TimeUnit.MILLISECONDS);
        }
        // sliding interval
        if (topoConf.containsKey(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)) {
            slidingIntervalCount = new BaseWindowedBolt.Count(((Number) topoConf.get(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT)).intValue());
        } else if (topoConf.containsKey(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)) {
            slidingIntervalDuration = new BaseWindowedBolt.Duration(((Number) topoConf.get(Configuration.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS)).intValue(), TimeUnit.MILLISECONDS);
        } else {
            // default is a sliding window of count 1
            slidingIntervalCount = new BaseWindowedBolt.Count(1);
        }
        if (topoConf.containsKey(Configuration.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
            throw new IllegalArgumentException("Late tuple stream can be defined only when specifying a timestamp field");
        }
        // validate
        validate(topoConf, windowLengthCount, windowLengthDuration,
                slidingIntervalCount, slidingIntervalDuration);
        EvictionPolicy<Tuple, ?> evictionPolicy = getEvictionPolicy(windowLengthCount, windowLengthDuration);
        TriggerPolicy<Tuple, ?> triggerPolicy = getTriggerPolicy(slidingIntervalCount, slidingIntervalDuration,
                manager, evictionPolicy);
        manager.setEvictionPolicy(evictionPolicy);
        manager.setTriggerPolicy(triggerPolicy);
        return manager;
    }

    private TriggerPolicy getTriggerPolicy(BaseWindowedBolt.Count slidingIntervalCount
            , BaseWindowedBolt.Duration slidingIntervalDuration
            , WindowManager<Tuple> manager, EvictionPolicy<Tuple, ?> evictionPolicy) {
        if (slidingIntervalCount != null) {
            return new CountTriggerPolicy(slidingIntervalCount.value, manager, evictionPolicy);
        } else {
            return new TimeTriggerPolicy(slidingIntervalDuration.value, manager, evictionPolicy);
        }
    }

    private EvictionPolicy<Tuple, ?> getEvictionPolicy(BaseWindowedBolt.Count windowLengthCount, BaseWindowedBolt.Duration windowLengthDuration) {
        if (windowLengthCount != null) {
            return new CountEvictionPolicy<>(windowLengthCount.value);
        } else {
            return new TimeEvictionPolicy<>(windowLengthDuration.value);
        }
    }

    private int getTopologyTimeoutMillis(Map<String, Object> topoConf) {
        if (topoConf.get(Configuration.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) != null) {
            boolean timeOutsEnabled = (boolean) topoConf.get(Configuration.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
            if (!timeOutsEnabled) {
                return Integer.MAX_VALUE;
            }
        }
        int timeout = 0;
        if (topoConf.get(Configuration.TOPOLOGY_MESSAGE_TIMEOUT_SECS) != null) {
            timeout = ((Number) topoConf.get(Configuration.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        }
        return timeout * 1000;
    }

    private void ensureDurationLessThanTimeout(int duration, int timeout) {
        if (duration > timeout) {
            throw new IllegalArgumentException("Window duration (length + sliding interval) value_list " + duration +
                    " is more than " + Configuration.TOPOLOGY_MESSAGE_TIMEOUT_SECS +
                    " value_list " + timeout);
        }
    }

    private void validate(Map<String, Object> topoConf, BaseWindowedBolt.Count windowLengthCount, BaseWindowedBolt.Duration windowLengthDuration,
                          BaseWindowedBolt.Count slidingIntervalCount, BaseWindowedBolt.Duration slidingIntervalDuration) {
        int topologyTimeout = getTopologyTimeoutMillis(topoConf);
        if (windowLengthCount == null && windowLengthDuration == null) {
            throw new IllegalArgumentException("Window length is not specified");
        }
        if (windowLengthDuration != null && slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value + slidingIntervalDuration.value, topologyTimeout);
        } else if (windowLengthDuration != null) {
            ensureDurationLessThanTimeout(windowLengthDuration.value, topologyTimeout);
        } else if (slidingIntervalDuration != null) {
            ensureDurationLessThanTimeout(slidingIntervalDuration.value, topologyTimeout);
        }
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        WindowLifecycleListener<Tuple> listener = newWindowLifecycleListener();
        this.windowManager = initWindowManager(listener, stormConf, context, new ConcurrentLinkedQueue<>());
    }

    private WindowLifecycleListener<Tuple> newWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> events) {
                /*
                 * NO-OP1: the events are ack-ed in execute
                 */
            }

            @Override
            public void onActivation(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples, Long timestamp) {
                boltExecute(tuples, newTuples, expiredTuples, timestamp);
            }
        };
    }

    private void boltExecute(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples, Long timestamp) {
        _op.execute(new TupleWindowImpl(tuples, newTuples, expiredTuples, getWindowStartTs(timestamp), timestamp));
    }

    public void execute(JumboTuple in) throws InterruptedException {
        for (int i = 0; i < in.length; i++) {
            final Tuple input = in.getTuple(i);
            windowManager.add(input);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not supported yet.
    }

    private Long getWindowStartTs(Long endTs) {
        Long res = null;
        if (endTs != null && windowLengthDuration != null) {
            res = endTs - windowLengthDuration.value;
        }
        return res;
    }

}
