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

import application.datatype.AccidentNotification;
import application.datatype.PositionReport;
import application.datatype.internal.AccidentTuple;
import application.datatype.util.Constants;
import application.datatype.util.ISegmentIdentifier;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Message;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * {@link AccidentNotificationBolt} notifies vehicles approaching an accident (ie, are at most 4 segments upstream of
 * the accident) to allow them to exit the express way. Vehicles are notified about accidents that occurred in the
 * minute before the current {@link PositionReport} (ie, 'minute number' [see Time.getMinute(short)])) and as long as
 * the accident was not cleared and only once per segment (ie, each time a new segment is entered).<br />
 * <br />
 * {@link AccidentNotificationBolt} processes two input streams. The first input is expected to be of type
 * {@link PositionReport} and must be grouped by vehicle id. The second input is expected to be of type
 * {@link AccidentTuple} and must be broadcasted. Both inputs most be ordered by time (ie, timestamp for
 * {@link PositionReport} and minute number for {@link AccidentTuple}). It is further assumed, that all
 * {@link AccidentTuple}s with a <em>smaller</em> minute number than a {@link PositionReport} tuple are delivered
 * <em>before</em> those {@link PositionReport}s.<br />
 * <br />
 * This implementation assumes, that {@link PositionReport}s are delivered via a stream called
 * {@link LRTopologyControl#POSITION_REPORTS_STREAM_ID}. Input tuples of all other streams are assumed to be
 * {@link AccidentTuple}s.<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport} and {@link AccidentTuple}<br />
 * <strong>Output schema:</strong> {@link AccidentNotification}
 *
 * @author mjsax
 * @author trillian
 */
public class AccidentNotificationBolt extends filterBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LoggerFactory.getLogger(AccidentNotificationBolt.class);
    /**
     * Contains all vehicle IDs and segment of the last {@link PositionReport} to allow skipping already sent
     * notifications (there's only one notification per segment per vehicle).
     */
    private final Map<Integer, Short> allCars = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentToCheck = new SegmentIdentifier();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private AccidentTuple inputAccidentTuple = new AccidentTuple();
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> currentMinuteAccidents = new HashSet<>();
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> previousMinuteAccidents = new HashSet<>();
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public AccidentNotificationBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID2, 1.0);
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, 0.0);
        this.setStateful();
    }


    private void execute_core() throws InterruptedException {

        this.checkMinute(this.inputPositionReport.getMinuteNumber());

        if (this.inputPositionReport.isOnExitLane()) {
            LOG.trace("ACCNotification,this.inputPositionReport is on ExistLane:" + this.inputPositionReport.toString());
            return;
        }

        final Short currentSegment = this.inputPositionReport.getSegment();
        final Integer vid = this.inputPositionReport.getVid();
        final Short previousSegment = this.allCars.put(vid, currentSegment);
        if (previousSegment != null && currentSegment.shortValue() == previousSegment.shortValue()) {
            return;
        }

        // upstream is either larger or smaller of current segment
        final Short direction = this.inputPositionReport.getDirection();
        final short dir = direction;
        // EASTBOUND == 0 => diff := 1
        // WESTBOUNT == 1 => diff := -1
        final short diff = (short) -(dir - 1 + ((dir + 1) / 2));
//				assert (dir == Constants.EASTBOUND ? diff == 1 : diff == -1);
        if (diff != 1 && diff != -1) {
            return;//the position report contains error message.
        }

        final Integer xway = this.inputPositionReport.getXWay();
        final short curSeg = currentSegment;

        this.segmentToCheck.setXWay(xway);
        this.segmentToCheck.setDirection(direction);

        for (int _i = 0; _i <= 4; ++_i) {
            final short nextSegment = (short) (curSeg + (diff * _i));
            assert (dir == Constants.EASTBOUND ? nextSegment >= curSeg : nextSegment <= curSeg);

            this.segmentToCheck.setSegment(nextSegment);

            if (this.previousMinuteAccidents.contains(this.segmentToCheck)) {
                // TODO GetAndUpdate accurate emit time...

                this.collector.emit(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID
                        , -1, new AccidentNotification(this.inputPositionReport.getTime(),
                                this.inputPositionReport.getTime(), this.segmentToCheck.getSegment(), vid));
                cnt1++;
                break; // send a notification for the closest accident only
            }
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        if (in.getSourceStreamId().equals(LRTopologyControl.POSITION_REPORTS_STREAM_ID)) {
            //this.inputPositionReport.clear();
            //Collections.addAll(this.inputPositionReport, in.getMsg(i).getValue(0));
            this.inputPositionReport = (PositionReport) in.getValue(0);
            execute_core();
        } else {//from AccidentDetection Bolt.
//				this.inputAccidentTuple.clear();
//				Collections.addAll(this.inputAccidentTuple, in.getMsg(i).getValue(0));
            this.inputAccidentTuple = (AccidentTuple) in.getValue(0);
            this.checkMinute(this.inputAccidentTuple.getMinuteNumber());
//				assert (this.inputAccidentTuple.getMinuteNumber() == this.currentMinute);
            this.currentMinuteAccidents.add(new SegmentIdentifier(this.inputAccidentTuple));
//				cnt2++;
        }
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            Message msg = in.getMsg(i);

            if (in.getSourceStreamId(i).equals(LRTopologyControl.POSITION_REPORTS_STREAM_ID)) {
                //this.inputPositionReport.clear();
                //Collections.addAll(this.inputPositionReport, in.getMsg(i).getValue(0));
                this.inputPositionReport = (PositionReport) msg.getValue(0);
                execute_core();
            } else {//from AccidentDetection Bolt.
//				this.inputAccidentTuple.clear();
//				Collections.addAll(this.inputAccidentTuple, in.getMsg(i).getValue(0));
                this.inputAccidentTuple = (AccidentTuple) msg.getValue(0);
                this.checkMinute(this.inputAccidentTuple.getMinuteNumber());
//				assert (this.inputAccidentTuple.getMinuteNumber() == this.currentMinute);
                this.currentMinuteAccidents.add(new SegmentIdentifier(this.inputAccidentTuple));
//				cnt2++;
            }
        }
        cnt += bound;
    }


    private void checkMinute(short minute) {
//		if (minute < this.currentMinute){
//			System.out.println("");
//		}
        if (minute < this.currentMinute) {
            //restart..
            currentMinute = minute;
        }

        if (minute > this.currentMinute) {
            LOG.trace("New minute: {}", minute);
            this.currentMinute = minute;
            this.previousMinuteAccidents = this.currentMinuteAccidents;
            this.currentMinuteAccidents = new HashSet<>();
        }
    }

    public void display() {

        LOG.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt)
                + "\tcnt2:" + cnt2 + "\tbranch selectivity:" + ((cnt2) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, AccidentNotification.getSchema());
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

    }

}
