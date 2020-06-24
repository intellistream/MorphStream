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

import application.datatype.*;
import application.datatype.util.LRTopologyControl;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static application.datatype.util.LRTopologyControl.POSITION_REPORTS_STREAM_ID;


/**
 * {@link DispatcherBolt_latency} retrieves a stream of {@code <ts,string>} tuples, parses the second CSV attribute and emits an
 * appropriate LRB tuple. The LRB input CSV schema is:
 * {@code Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, S_init, S_end, DOW, TOD, Day}<br />
 * <br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link PositionReport} (stream: {@link LRTopologyControl#POSITION_REPORTS_STREAM_ID})</li>
 * <li>{@link AccountBalanceRequest} (stream: {@link LRTopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID})</li>
 * <li>{@link DailyExpenditureRequest} (stream: {@link LRTopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID})</li>
 * <li>{@link TravelTimeRequest} (stream: {@link LRTopologyControl#TRAVEL_TIME_REQUEST_STREAM_ID})</li>
 * </ul>
 *
 * @author mjsax
 **/
public class DispatcherBolt_latency extends filterBolt {
    private static final long serialVersionUID = 6908631355830501961L;
    private static final Logger LOG = LoggerFactory.getLogger(DispatcherBolt_latency.class);

    long cnt = 0, de = 0, pr = 0, ab = 0;
    // 10215332
    // private double pr = 5641745.0 (55.2%), ab = 2287782.0 (22.4%), de = 2285805.0 (22.4%);

    /**
     * outputFieldsDeclarer.declareStream(LRTopologyControl.POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
     * outputFieldsDeclarer.declareStream(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, AccountBalanceRequest.getSchema());
     * outputFieldsDeclarer.declareStream(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, DailyExpenditureRequest.getSchema());
     */


    public DispatcherBolt_latency() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(POSITION_REPORTS_STREAM_ID, 0.9885696197046802);
//		this.output_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, 0.0057618584512478315);
//		this.output_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, 0.0011689623684645518);

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
//		long pre_pr = pr;
//		cnt += bound;
        for (int i = 0; i < bound; i++) {
            String raw = null;

//			try {
            raw = in.getString(0, i);
            //raw = raw.substring(3, raw.length() - 2);
            String[] token = raw.split(" ");
            // common attributes of all in tuples
            short type = Short.parseShort(token[0]);
            Short time = Short.parseShort(token[1]);
            Integer vid = Integer.parseInt(token[2]);
            assert (time.shortValue() == Short.parseShort(token[1]));

            if (type == AbstractLRBTuple.position_report) {

//					long _bid = BIDGenerator2.getHolder().getAndIncrement();
                long msgID = in.getLong(1, i);

                if (msgID != -1) {
                    this.collector.emit(POSITION_REPORTS_STREAM_ID,
                            bid,
//							gap,
                            new PositionReport(// position
                                    time,//
                                    vid,//
                                    Integer.parseInt(token[3]), // speed
                                    Integer.parseInt(token[4]), // xway
                                    Short.parseShort(token[5]), // lane
                                    Short.parseShort(token[6]), // direction
                                    Short.parseShort(token[7]), // segment
                                    Integer.parseInt(token[8])
                            ), msgID,
                            in.getLong(2, i)
                    );
                } else {
                    this.collector.emit(POSITION_REPORTS_STREAM_ID,
                            bid,
//							gap,
                            new PositionReport(// position
                                    time,//
                                    vid,//
                                    Integer.parseInt(token[3]), // speed
                                    Integer.parseInt(token[4]), // xway
                                    Short.parseShort(token[5]), // lane
                                    Short.parseShort(token[6]), // direction
                                    Short.parseShort(token[7]), // segment
                                    Integer.parseInt(token[8])
                            ), msgID,
                            0
                    );
                }

//				pr++;
            } else {
                // common attribute of all requests
//				Integer qid = Integer.parseInt(token[9]);
                switch (type) {
                    case AbstractLRBTuple.account_balance_request:
                        break;
                    case AbstractLRBTuple.daily_expenditure_request:
                        break;
                    case AbstractLRBTuple.travel_time_request://not in use in this experiment.
                        break;
                    default:
//						LOG.error("Unkown tuple type: {}", type);
                }
            }
        }
    }


    public void display() {
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(POSITION_REPORTS_STREAM_ID, PositionReport.getLatencySchema());
    }

}
