/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */

package application.bolts.lr;


import application.datatype.PositionReport;
import application.datatype.internal.AvgVehicleSpeedTuple;
import application.datatype.util.AvgValue;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import application.util.Time;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * {@link AverageVehicleSpeedBolt} computes the average speed of a vehicle within an express way-segment (partition
 * direction) every minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and
 * must be grouped by vehicle. A new average speed computation is trigger each time a vehicle changes the express way,
 * segment or direction as well as each 60 seconds (ie, changing 'minute number' [see {@link Time#getMinute(long)}]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link AvgVehicleSpeedTuple}
 *
 * @author msoyka
 * @author mjsax
 */
public class AverageVehicleSpeedBolt extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageVehicleSpeedBolt.class);


    /**
     * Internally (re)used object to access individual attributes.
     */
    private final PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();

    /**
     * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<Integer, Pair<AvgValue, SegmentIdentifier>>();

    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    public AverageVehicleSpeedBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.setStateful();
    }


    private void update_avgsv(long bid, Integer vid, int speed, Short time) throws InterruptedException {
        Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);

        if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {// vehicle changes segment.

            SegmentIdentifier segId = vehicleEntry.getRight();

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            this.collector.emit(bid, new AvgVehicleSpeedTuple(vid, this.currentMinute,
                    segId.getXWay(), segId.getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage(), time));

            // set to null to GetAndUpdate new vehicle entry below
            vehicleEntry = null;
        }

        if (vehicleEntry == null) {//no record for this vehicle
            //write (insert).
            vehicleEntry = new MutablePair<>(new AvgValue(speed), this.segment.copy());
            this.avgSpeedsMap.put(vid, vehicleEntry);

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            this.collector.emit(bid,
                    new AvgVehicleSpeedTuple(vid, this.currentMinute,
                            segment.getXWay(), segment.getSegment(),
                            segment.getDirection(), vehicleEntry.getLeft().getAverage(), time));

        } else {// vehicle does not change segment but only GetAndUpdate its speed.
            //write.
            vehicleEntry.getLeft().updateAverage(speed);

            // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
            this.collector.emit(bid,
                    new AvgVehicleSpeedTuple(vid, this.currentMinute,
                            segment.getXWay(), segment.getSegment(),
                            segment.getDirection(), vehicleEntry.getLeft().getAverage(),
                            time
                    ));
        }
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        this.inputPositionReport.clear();
        this.inputPositionReport.addAll(input.getValues());
        LOGGER.trace(this.inputPositionReport.toString());
        int vid = this.inputPositionReport.getVid();
        int speed = this.inputPositionReport.getSpeed().intValue();
        this.segment.set(this.inputPositionReport);
        update_avgsv(input.getBID(), vid, speed, inputPositionReport.getTime());
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(AvgVehicleSpeedTuple.getSchema());
    }

}
