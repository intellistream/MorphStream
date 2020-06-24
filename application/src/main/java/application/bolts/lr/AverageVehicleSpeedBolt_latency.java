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
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * {@link AverageVehicleSpeedBolt_latency} computes the average speed of a vehicle within an express way-segment (partition
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
public class AverageVehicleSpeedBolt_latency extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageVehicleSpeedBolt_latency.class);
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each vehicle to its average speed value_list that corresponds to the current 'minute number' and specified
     * segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public AverageVehicleSpeedBolt_latency() {
        super(LOGGER, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, 0.999999996788937);
        this.setStateful();
    }


    @Override
    public void execute(Tuple in) throws InterruptedException {
//       not in use
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {


    }


    public void display() {
//		LOGGER.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\tcnt2:" + cnt2 + "\toutput selectivity:" + ((cnt1 + cnt2) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, AvgVehicleSpeedTuple.getLatencySchema());
    }

}
