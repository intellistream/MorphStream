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
import application.datatype.TollNotification;
import application.datatype.internal.AccidentTuple;
import application.datatype.internal.CountTuple;
import application.datatype.internal.LavTuple;
import application.datatype.util.ISegmentIdentifier;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static application.datatype.util.LRTopologyControl.CAR_COUNTS_STREAM_ID;

/**
 * {@link TollNotificationBolt} calculates the toll for each vehicle and reports it back to the vehicle if a vehicle
 * enters a segment. Furthermore, the toll is assessed to the vehicle if it leaves a segment.<br />
 * <br />
 * The toll depends on the number of cars in the segment (the minute before) the car is driving on and is only charged
 * if the car is not on the exit line, more than 50 cars passed this segment the minute before, the
 * "latest average velocity" is smaller then 40, and no accident occurred in the minute before in the segment and 4
 * downstream segments.<br />
 * <br />
 * {@link TollNotificationBolt} processes four input streams. The first input is expected to be of type
 * {@link PositionReport} and must be grouped by vehicle id. The other inputs are expected to be of type
 * {@link AccidentTuple}, {@link CountTuple}, and {@link LavTuple} and must be broadcasted. All inputs most be ordered
 * by time (ie, timestamp for {@link PositionReport} and minute number for {@link AccidentTuple}, {@link CountTuple},
 * and {@link LavTuple}). It is further assumed, that all {@link AccidentTuple}s and {@link CountTuple}s with a
 * <em>smaller</em> minute number than a {@link PositionReport} tuple as well as all {@link LavTuple}s with the
 * <em>same</em> minute number than a {@link PositionReport} tuple are delivered <em>before</em> those
 * {@link PositionReport}s.<br />
 * <br />
 * This implementation assumes, that {@link PositionReport}s, {@link AccidentTuple}s, {@link CountTuple}s, and
 * {@link LavTuple}s are delivered via streams called {@link LRTopologyControl#POSITION_REPORTS_STREAM_ID},
 * {@link LRTopologyControl#ACCIDENTS_STREAM_ID},LRTopologyControl#ACCIDENTS_STREAM_ID2}, {@link LRTopologyControl#CAR_COUNTS_STREAM_ID}, and
 * {@link LRTopologyControl#LAVS_STREAM_ID}, respectively.<br />
 * <br />
 * <strong>Expected input:</strong> {@link PositionReport}, {@link AccidentTuple}, {@link CountTuple}, {@link LavTuple}<br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link TollNotification} (stream: {@link LRTopologyControl#TOLL_NOTIFICATIONS_STREAM_ID})</li>
 * <li>{@link TollNotification} (stream: {@link LRTopologyControl#TOLL_ASSESSMENTS_STREAM_ID})</li>
 * </ul>
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class TollNotificationBolt extends filterBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TollNotificationBolt.class);

    /**
     * Average vehicle count of a road.
     */
    private Map<SegmentIdentifier, Integer> Road_Cnt = new HashMap<SegmentIdentifier, Integer>();

    /**
     * Average traffic speed of a road.
     */
    private Map<ISegmentIdentifier, Double> Road_Speed = new HashMap<>();

    /**
     * Internally (re)used object to access individual attributes.
     */
    private final PositionReport inputPositionReport = new PositionReport();

    /**
     * Internally (re)used object to access individual attributes.
     */
    private final CountTuple inputCountTuple = new CountTuple();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final LavTuple inputLavTuple = new LavTuple();

    public TollNotificationBolt() {
        super(LOGGER, new HashMap<>(), new HashMap<>());//let input selectivity to control.
        this.input_selectivity.put(CAR_COUNTS_STREAM_ID, 1.0);//produce no output..
        this.input_selectivity.put(LRTopologyControl.LAVS_STREAM_ID, 1.0); //
        this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
        this.setStateful();
    }


    private void toll_process(Integer vid, Integer count, Double lav) throws InterruptedException {
        int toll = 0;

        if (lav < 40) {

            int carCount = 0;
            if (count != null) {
                carCount = count.intValue();
            }

            if (carCount > 50) {

                //TODO: check accident. not in use in this experiment.
                    /*
                     downstream is either larger or smaller of current segment
                    final Short direction = this.inputPositionReport.getDirection();
                    final short dir = direction.shortValue();
                     EASTBOUND == 0 => diff := 1
                     WESTBOUNT == 1 => diff := -1
                    final short diff = (short) -(dir - 1 + ((dir + 1) / 2));
                    assert (dir == Constants.EASTBOUND.shortValue() ? diff == 1 : diff == -1);

                    final Integer xway = this.inputPositionReport.getXWay();
                    final short curSeg = currentSegment.shortValue();

                    this.segmentToCheck.setXWay(xway);
                    this.segmentToCheck.setDirection(direction);


                    int i;
                    for (i = 0; i <= 4; ++i) {
                        final short nextSegment = (short) (curSeg + (diff * i));
                        assert (dir == Constants.EASTBOUND.shortValue() ? nextSegment >= curSeg : nextSegment <= curSeg);

                        this.segmentToCheck.setSegment(new Short(nextSegment));

                        if (this.previousMinuteAccidents.contains(this.segmentToCheck)) {//???
                            break;
                        }
                    }
                    */

                { // only true if no accident was found and "break" was not executed
                    final int var = carCount - 50;
                    toll = 2 * var * var;
                }
            }
        }

        // TODO GetAndUpdate accurate emit time...
        final TollNotification tollNotification
                = new TollNotification(this.inputPositionReport.getTime(),
                this.inputPositionReport.getTime(), vid, lav, toll);

        this.collector.emit(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, tollNotification);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        final String inputStreamId = input.getSourceStreamId();

        if (inputStreamId.equals(LRTopologyControl.POSITION_REPORTS_STREAM_ID)) {
            this.inputPositionReport.clear();
            this.inputPositionReport.addAll(input.getValues());


            final Integer vid = this.inputPositionReport.getVid();
            SegmentIdentifier segment = new SegmentIdentifier(this.inputPositionReport);//which road segment is this report referring to.


            //read state.
            Double lav = this.Road_Speed.get(segment);
            final Integer count = this.Road_Cnt.get(segment);


            if (lav != null) {
            } else {
                lav = -1d;
            }

            toll_process(vid, count, lav);


        } else if (inputStreamId.equals(LRTopologyControl.CAR_COUNTS_STREAM_ID)) {//GetAndUpdate counts.
            this.inputCountTuple.clear();
            this.inputCountTuple.addAll(input.getValues());
            this.Road_Cnt.put(new SegmentIdentifier(this.inputCountTuple), this.inputCountTuple.getCount());

        } else if (inputStreamId.equals(LRTopologyControl.LAVS_STREAM_ID)) {//GetAndUpdate speeds.
            this.inputLavTuple.clear();
            this.inputLavTuple.addAll(input.getValues());
            this.Road_Speed.put(new SegmentIdentifier(this.inputLavTuple), this.inputLavTuple.getLav());
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, TollNotification.getSchema());
//        declarer.declareStream(LRTopologyControl.TOLL_ASSESSMENTS_STREAM_ID, TollNotification.getSchema());
    }

}
