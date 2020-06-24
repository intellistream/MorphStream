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
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link LatestAverageVelocityBolt_latency} computes the "latest average velocity" (LAV), ie, the average speed over all
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
 */
public class LatestAverageVelocityBolt_latency extends LatestAverageVelocityBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LatestAverageVelocityBolt_latency.class);

    /**
     * Holds the (at max) last five average speed value_list for each segment.
     */
    private final Map<SegmentIdentifier, List<Double>> averageSpeedsPerSegment = new HashMap<>();

    /**
     * Holds the (at max) last five minute numbers for each segment.
     */
    private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public LatestAverageVelocityBolt_latency() {
        super();
        this.input_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0);
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//        not in use.
    }




    public void display() {

        LOGGER.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.LAVS_STREAM_ID, LavTuple.getLatencySchema());
    }

}
