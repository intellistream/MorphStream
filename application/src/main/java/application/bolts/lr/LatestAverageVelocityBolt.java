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

import application.datatype.internal.AvgSpeedTuple;
import application.datatype.internal.AvgVehicleSpeedTuple;
import application.datatype.internal.LavTuple;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link LatestAverageVelocityBolt} computes the "latest average velocity" (LAV), ie, the average speed over all
 * vehicle within an express way-segment (partition direction), over the last five minutes (see Time.getMinute(short)]).
 * The input is expected to be of type {@link AvgSpeedTuple}, to be ordered by timestamp, and must be grouped by
 * {@link SegmentIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link LavTuple} ( (stream: {@link LRTopologyControl#LAVS_STREAM_ID})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 * <p>
 * TODO: For simplicity, we only keep 1 elements.. that is window length=1, slide=1. The original LR benchmark requires to keep "last five minutes" @author Tony
 */
public class LatestAverageVelocityBolt extends filterBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt.class);

//    /**
//     * Internally (re)used object to access individual attributes.
//     */
//    private final AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();

    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();

    /**
     * Holds the average speed value for each segment.
     */
    private final Map<SegmentIdentifier, Double> averageSpeedsPerSegment = new HashMap<>();


    public LatestAverageVelocityBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0);
        this.setStateful();
    }


    private void update_avgs(long bid, double speed, short time) throws InterruptedException {

        Double latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segment);

        if (latestAvgSpeeds == null) {
            this.averageSpeedsPerSegment.put(this.segment.copy(), speed);//initialize speed
            latestAvgSpeeds = speed;
        }


        //compute lav
        double lav = (latestAvgSpeeds + speed) / 2;//compute the average.

        //broadcast the updated average road speed to TN.
        this.collector.emit(LRTopologyControl.LAVS_STREAM_ID,
                bid,
                new LavTuple((short) (-1),//remove minutes
                        this.segment.getXWay(), this.segment.getSegment(),
                        this.segment.getDirection(), lav, time)


        );
    }


    @Override
    public void execute(Tuple input) throws InterruptedException {
        AvgVehicleSpeedTuple inputTuple = (AvgVehicleSpeedTuple) input.getValues();
        this.segment.set(inputTuple);
        Double avgSpeed = inputTuple.getAvgSpeed();
        update_avgs(input.getBID(), avgSpeed, inputTuple.getTime());
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.LAVS_STREAM_ID, LavTuple.getSchema());
//        declarer.declareStream(TimestampMerger.FLUSH_STREAM_ID, new Fields("ts"));
    }

}
