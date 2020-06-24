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
import application.datatype.internal.CountTuple;
import application.datatype.util.CarCount;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * {@link CountVehiclesBolt_latency} counts the number of vehicles within an express way segment (partition direction) every
 * minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and must be grouped
 * by {@link SegmentIdentifier}. A new count value_list is emitted each 60 seconds (ie, changing 'minute number' [see
 * Time.getMinute(short)]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link CountTuple} (stream: {@link LRTopologyControl#CAR_COUNTS_STREAM_ID})
 *
 * @author mjsax
 */
public class CountVehiclesBolt_latency extends filterBolt {
    private static final long serialVersionUID = 6158421247331445466L;
    private static final Logger LOG = LoggerFactory.getLogger(CountVehiclesBolt_latency.class);
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each segment to its count value_list.
     */
    private final Map<SegmentIdentifier, CarCount> countsMap = new HashMap<>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = -1;

    private double cnt = 0, cnt1 = 0;
//	TimestampMerger merger;

    public CountVehiclesBolt_latency() {
        super(LOG, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.CAR_COUNTS_STREAM_ID, 1.0);
        this.setStateful();
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//     not in use.
    }


    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
//			cnt++;
//			this.inputPositionReport.clear();


//			Long msgId;
//			Long SYSStamp;
//			msgId = in.getLong(1, i);
//			SYSStamp = in.getLong(2, i);

            this.inputPositionReport = (PositionReport) in.getMsg(i).getValue(0);
            LOG.trace(this.inputPositionReport.toString());

            short minute = this.inputPositionReport.getMinuteNumber();
            this.segment.set(this.inputPositionReport);

            //assert (minute >= this.currentMinute);


//			if (minute > this.currentMinute) {
            boolean emitted = false;
            // emit all values for last minute
            // (because in tuples are ordered by ts (ie, minute number), we can relax_reset the last minute safely)
            for (Entry<SegmentIdentifier, CarCount> entry : this.countsMap.entrySet()) {
                SegmentIdentifier segId = entry.getKey();

                // Minute-Number, X-Way, Segment, Direction, Avg(speed)
                int count = entry.getValue().size();
                if (count > 50) {//?
//						cnt1++;
                    emitted = true;
                    long msgID = in.getLong(1, i);
                    if (msgID != -1) {

                        this.collector.emit(LRTopologyControl.CAR_COUNTS_STREAM_ID, bid

                                , new CountTuple(this.currentMinute, segId.getXWay(), segId.getSegment(), segId.getDirection(), count,
                                        inputPositionReport.getTime()), msgID, in.getLong(2, i)
                        );
                    } else {
                        this.collector.emit(LRTopologyControl.CAR_COUNTS_STREAM_ID, bid

                                , new CountTuple(this.currentMinute, segId.getXWay(), segId.getSegment(), segId.getDirection(), count,
                                        inputPositionReport.getTime()), msgID, 0
                        );
                    }

                }
            }
            if (!emitted) {
//					cnt1++;
                long msgID = in.getLong(1, i);
                if (msgID != -1) {
                    this.collector.emit(LRTopologyControl.CAR_COUNTS_STREAM_ID
                            , bid, new CountTuple(minute), msgID, in.getLong(2, i));
                } else {
                    this.collector.emit(LRTopologyControl.CAR_COUNTS_STREAM_ID
                            , bid, new CountTuple(minute), msgID, 0);
                }
            }
            this.countsMap.clear();
            this.currentMinute = minute;
//			}

            CarCount segCnt = this.countsMap.get(this.segment);
            if (segCnt == null) {
                segCnt = new CarCount();
                this.countsMap.put(this.segment.copy(), segCnt);
            } else {
//                ++segCnt.count;//TODO: to be updated
            }
        }
    }

    public void display() {

        LOG.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
//		merger = new TimestampMerger(this, PositionReport.MIN_IDX);
//		merger.prepare(config, context, this.collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.CAR_COUNTS_STREAM_ID, CountTuple.getLatencySchema());
    }
}
