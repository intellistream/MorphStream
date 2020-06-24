package application.bolts.sd;

import application.constants.SpikeDetectionConstants;
import application.util.Configuration;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class MovingAverageBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageBolt.class);
    private static final long serialVersionUID = -8453666140979888684L;
    int loop = 1;
    int cnt = 0;
    LinkedList<Double> valueList;
    private int movingAverageWindow;
    private Map<Integer, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<Integer, Double> deviceIDtoSumOfEvents;

    public MovingAverageBolt() {
        super(LOG);
        this.setStateful();
        this.read_selectivity = 2.0;
    }

    @Override
    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 37;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        movingAverageWindow = config.getInt(SpikeDetectionConstants.Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
//		final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
//			int deviceID = in.getInt(0, i);
//			double nextDouble = in.getDouble(1, i);
//			double movingAvergeInstant = movingAverage(in.getInt(0, i), in.getDouble(1, i));

            int deviceID = in.getInt(0, i);
            double nextDouble = in.getDouble(1, i);

            collector.emit(0, deviceID, movingAverage(deviceID, nextDouble), nextDouble);
        }
    }

    @Override
    public void profile_execute(JumboTuple in) {
        int bound = in.length;
//		final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            int deviceID = in.getInt(0, i);
            double nextDouble = in.getDouble(1, i);

            collector.emit_nowait(deviceID, movingAverage(deviceID, nextDouble), nextDouble);
        }
    }

    private double movingAverage(int deviceID, double nextDouble) {
        double sum = 0.0;
        valueList = new LinkedList<>();
        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow - 1) {
                double valueToRemove = valueList.removeFirst();
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum / valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(SpikeDetectionConstants.Field.DEVICE_ID, SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE);
    }
}