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
import application.datatype.internal.AvgSpeedTuple;
import application.datatype.internal.AvgVehicleSpeedTuple;
import application.datatype.util.AvgValue;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import application.util.Time;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * {@link AverageSpeedBolt} computes the average speed of a vehicle within an express way-segment (partition
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
public class AverageSpeedBolt extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageSpeedBolt.class);


    /**
     * Internally (re)used object to access individual attributes.
     */
    private final AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();

    /**
     * Maps each segment to its average speed value.
     */
    private final Map<SegmentIdentifier, AvgValue> avgSpeedsMap = new HashMap<SegmentIdentifier, AvgValue>();

    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = -1;


    public AverageSpeedBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.setStateful();
    }


    private void update_avgs(double avgVehicleSpeed) {

        //read
        AvgValue segAvg = this.avgSpeedsMap.get(this.segment);

        if (segAvg == null) {
            segAvg = new AvgValue(avgVehicleSpeed);
            this.avgSpeedsMap.put(this.segment.copy(), segAvg);
        } else {
            segAvg.updateAverage(avgVehicleSpeed);
        }
    }


    //simplified, assume input tuples are (mostly) sorted by time.
    @Override
    public void execute(Tuple input) throws InterruptedException {
//        if (input.getSourceStreamId().equals(TimestampMerger.FLUSH_STREAM_ID)) {
//            Object ts = input.getValue(0);
//            if (ts == null) {
//                this.flushBuffer();
//                this.collector.force_emit(TimestampMerger.FLUSH_STREAM_ID, new StreamValues((Object) null));
//            } else {
//                this.checkMinute(((Number) ts).shortValue());
//            }
////            this.collector.ack(input);
//            return;
//        }

        this.inputTuple.clear();
        this.inputTuple.addAll(input.getValues());
        LOGGER.trace(this.inputTuple.toString());

//        this.checkMinute(this.inputTuple.getMinute().shortValue());

        this.segment.set(this.inputTuple);//segment
        double avgVehicleSpeed = this.inputTuple.getAvgSpeed().doubleValue();//average vehicle speed

        update_avgs(avgVehicleSpeed);

//      this.collector.ack(input);

    }

//    private void checkMinute(short minute) throws InterruptedException {
//        assert (minute >= this.currentMinute);
//
//        if (minute > this.currentMinute) {
//            // emit all values for last minute
//            // (because input tuples are ordered by ts (ie, minute number), we can close the last minute safely)
//            this.flushBuffer();
//            this.collector.force_emit(TimestampMerger.FLUSH_STREAM_ID, new StreamValues(new Short(minute)));
//            this.avgSpeedsMap.clear();
//            this.currentMinute = minute;
//        }
//    }

//    private void flushBuffer() throws InterruptedException {
//        for (Map.Entry<SegmentIdentifier, AvgValue> entry : this.avgSpeedsMap.entrySet()) {
//            SegmentIdentifier segId = entry.getKey();
//            // Minute-Number, X-Way, Segment, Direction, Avg(speed)
//            this.collector.force_emit(DEFAULT_STREAM_ID, new AvgSpeedTuple(new Short(this.currentMinute), segId.getXWay(), segId.getSegment(),
//                    segId.getDirection(), entry.getValue().getAverage()));
//        }
//    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(AvgSpeedTuple.getSchema());
//        declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
    }

}
