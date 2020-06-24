package application.bolts.tm;

import application.constants.TrafficMonitoringConstants;
import application.model.gis.Road;
import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn
 */
public class SpeedCalculatorBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculatorBolt.class);
    private static final long serialVersionUID = -918188615007384226L;
    int loop = 1;
    private Map<Integer, Road> roads;
    private double cnt = 0;//stop SC execution
    private double cnt1 = 0;

    public SpeedCalculatorBolt() {
        super(LOG, 0.64);//SC only reads a portion of data in the ttuple
        loops = 500;//this bolt is too slow, use smaller loops for it.
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        roads = new HashMap<>();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        int roadID = in.getIntegerByField(TrafficMonitoringConstants.Field.ROAD_ID);
        int speed = in.getIntegerByField(TrafficMonitoringConstants.Field.SPEED);

        int averageSpeed = 0;
        int count = 0;

        if (!roads.containsKey(roadID)) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);
            roads.put(roadID, road);
            averageSpeed = speed;
            count = 1;
        } else {

            Road road = roads.get(roadID);
            int sum = 0;
            if (road.getRoadSpeedSize() < 2) {
                road.incrementCount();
                road.addRoadSpeed(speed);

                for (int it : road.getRoadSpeed()) {
                    sum += it;
                }

                averageSpeed = (int) ((double) sum / (double) road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = roads.get(roadID).getAverageSpeed();
                double temp = 0;

                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it - avgLast), 2);
                }

                int avgCurrent = (int) ((sum + speed) / ((double) road.getRoadSpeedSize() + 1));
                temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                double stdDev = Math.sqrt(temp);

                if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);
                    road.setAverageSpeed(avgCurrent);

                    averageSpeed = avgCurrent;
                    count = road.getRoadSpeedSize();
                }
            }

        }
        collector.emit(bid, new StreamValues(new Date(), roadID, averageSpeed, count));
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final long bid = in.getBID();
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            int roadID = in.getIntegerByField(TrafficMonitoringConstants.Field.ROAD_ID, i);
            int speed = in.getIntegerByField(TrafficMonitoringConstants.Field.SPEED, i);

            int averageSpeed = 0;
            int count = 0;

            if (!roads.containsKey(roadID)) {
                Road road = new Road(roadID);
                road.addRoadSpeed(speed);
                road.setCount(1);
                road.setAverageSpeed(speed);
                roads.put(roadID, road);
                averageSpeed = speed;
                count = 1;
            } else {

                Road road = roads.get(roadID);
                int sum = 0;
                if (road.getRoadSpeedSize() < 2) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                    }

                    averageSpeed = (int) ((double) sum / (double) road.getRoadSpeedSize());
                    road.setAverageSpeed(averageSpeed);
                    count = road.getRoadSpeedSize();
                } else {
                    double avgLast = roads.get(roadID).getAverageSpeed();
                    double temp = 0;

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                        temp += Math.pow((it - avgLast), 2);
                    }

                    int avgCurrent = (int) ((sum + speed) / ((double) road.getRoadSpeedSize() + 1));
                    temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                    double stdDev = Math.sqrt(temp);

                    if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                        road.incrementCount();
                        road.addRoadSpeed(speed);
                        road.setAverageSpeed(avgCurrent);

                        averageSpeed = avgCurrent;
                        count = road.getRoadSpeedSize();
                    }
                }

            }
            collector.emit(bid, new StreamValues(new Date(), roadID, averageSpeed, count));


        }


    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(TrafficMonitoringConstants.Field.NOW_DATE, TrafficMonitoringConstants.Field.ROAD_ID, TrafficMonitoringConstants.Field.AVG_SPEED, TrafficMonitoringConstants.Field.COUNT);
    }
}
