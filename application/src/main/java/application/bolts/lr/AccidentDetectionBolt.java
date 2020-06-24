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
import application.datatype.internal.AccidentTuple;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.PositionIdentifier;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static application.datatype.util.LRTopologyControl.ACCIDENTS_STREAM_ID;

/**
 * {@link AccidentDetectionBolt} registers every stopped vehicle and emits accident information for further processing.
 * The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and must be grouped by
 * {@link PositionIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link AccidentTuple} (stream: {@link LRTopologyControl#ACCIDENTS_STREAM_ID ,LRTopologyControl#ACCIDENTS_STREAM_ID2})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class AccidentDetectionBolt extends filterBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LoggerFactory.getLogger(AccidentDetectionBolt.class);
    /**
     * Internally (re)used object.
     */
    private final PositionIdentifier vehiclePosition = new PositionIdentifier();
    /**
     * Internally (re)used object.
     */
    private final PositionIdentifier lastVehiclePosition = new PositionIdentifier();
    /**
     * Holds the last positions for each vehicle (if those positions are equal to each other).
     */
    private final Map<Integer, List<PositionReport>> lastPositions = new HashMap<>();
    /**
     * Hold all vehicles that have <em>stopped</em> within a segment.
     */
    private final Map<PositionIdentifier, Set<Integer>> stoppedCarsPerPosition = new HashMap<>();

    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

//	TimestampMerger merger;

    /**
     * almost no output..
     */
    public AccidentDetectionBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.POSITION_REPORTS_STREAM_ID, 1.0);//0.552
        this.output_selectivity.put(ACCIDENTS_STREAM_ID, 0.0);
//		this.output_selectivity.put(ACCIDENTS_STREAM_ID2, 1.0120297503497165E-4);
        this.setStateful();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
//		merger = new TimestampMerger(this, PositionReport.MIN_IDX);
//		merger.prepare(config, context, this.collector);

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        PositionReport inputPositionReport = (PositionReport) in.getValue(0);

        Integer vid = inputPositionReport.getVid();
        short minute = inputPositionReport.getMinuteNumber();

        if (minute < this.currentMinute) {
            //restart..
            currentMinute = minute;
        }
        if (minute > this.currentMinute) {//process next minutes.
            this.currentMinute = minute;
//			this.collector.force_emit(ACCIDENTS_STREAM_ID, new AccidentTuple(inputPositionReport.copy(), minute));
        }

        PositionReport lastPositionReport;
        if (inputPositionReport.isOnExitLane()) {
            List<PositionReport> vehiclePositions = this.lastPositions.remove(vid);

            if (vehiclePositions != null && vehiclePositions.size() == 4) {//???

                lastPositionReport = vehiclePositions.get(0);
                this.lastVehiclePosition.set(lastPositionReport);

                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                stoppedCars.remove(vid);
                if (stoppedCars.size() == 0) {
                    this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                }
            }
            return;
        }


        List<PositionReport> vehiclePositions = this.lastPositions.get(vid);
        if (vehiclePositions == null) {
            vehiclePositions = new LinkedList<>();
            vehiclePositions.add(inputPositionReport.copy());
            this.lastPositions.put(vid, vehiclePositions);
            return;
        }


        lastPositionReport = vehiclePositions.get(0);
        this.vehiclePosition.set(inputPositionReport);
        this.lastVehiclePosition.set(lastPositionReport);
        if (this.vehiclePosition.equals(this.lastVehiclePosition)) {
            vehiclePositions.add(0, inputPositionReport.copy());
            if (vehiclePositions.size() >= 4) {
                LOG.trace("Car {} stopped at {} ({})", vid, this.vehiclePosition,
                        inputPositionReport.getMinuteNumber());
                if (vehiclePositions.size() > 4) {
                    assert (vehiclePositions.size() == 5);
                    vehiclePositions.remove(4);
                }
                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.vehiclePosition);
                if (stoppedCars == null) {
                    stoppedCars = new HashSet<>();
                    stoppedCars.add(vid);
                    this.stoppedCarsPerPosition.put(this.vehiclePosition.copy(), stoppedCars);
                } else {
                    stoppedCars.add(vid);
                    PositionReport inputPositionReport_cp = inputPositionReport.copy();
                    if (stoppedCars.size() > 1) {
                        cnt2++;
//						this.collector.force_emit(
//								ACCIDENTS_STREAM_ID,

//								new AccidentTuple(inputPositionReport_cp, minute,
//										inputPositionReport_cp.getXWay(), inputPositionReport_cp.getSegment(),
//										inputPositionReport_cp.getDirection()));


                    }
                }

            }
        } else {
            if (vehiclePositions.size() == 4) {
                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                stoppedCars.remove(vid);
                if (stoppedCars.size() == 0) {
                    this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                }
            }
            vehiclePositions.clear();
            vehiclePositions.add(inputPositionReport.copy());
        }

    }


    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

//			this.inputPositionReport.clear();
//			Collections.addAll(this.inputPositionReport, in.getMsg(i).getValue(0));
			/*
	  Internally (re)used object to access individual attributes.
	 */
            PositionReport inputPositionReport = (PositionReport) in.getMsg(i).getValue(0);
//			//LOG.DEBUG("ACCDetection, this.inputPositionReport:" + inputPositionReport.toString());

            Integer vid = inputPositionReport.getVid();
            short minute = inputPositionReport.getMinuteNumber();

//			assert (minute >= this.currentMinute);

            if (minute < this.currentMinute) {
                //restart..
                currentMinute = minute;
            }
//			if (minute > this.currentMinute) {//process next minutes. work around.
            this.currentMinute = minute;
//				PositionReport inputPositionReport_cp = inputPositionReport.copy();
//				cnt1++;
//			this.collector.emit(ACCIDENTS_STREAM_ID, bid, new AccidentTuple(inputPositionReport.copy(), minute));
//			LOG.trace("ACCIDENTS detection (" + this.getContext().getThisTaskId() + ") emit:" + bid);
//			}

			/*
	  Internally (re)used object.
	 */
            PositionReport lastPositionReport;
            if (inputPositionReport.isOnExitLane()) {
                List<PositionReport> vehiclePositions = this.lastPositions.remove(vid);

                if (vehiclePositions != null && vehiclePositions.size() == 4) {//???
//					this.lastPositionReport.clear();
//					this.lastPositionReport.add(vehiclePositions.GetAndUpdate(0));
                    lastPositionReport = vehiclePositions.get(0);
                    this.lastVehiclePosition.set(lastPositionReport);

                    Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                    stoppedCars.remove(vid);
                    if (stoppedCars.size() == 0) {
                        this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                    }
                }
                return;
            }


            List<PositionReport> vehiclePositions = this.lastPositions.get(vid);
            if (vehiclePositions == null) {
                vehiclePositions = new LinkedList<>();
                vehiclePositions.add(inputPositionReport.copy());
                this.lastPositions.put(vid, vehiclePositions);
                return;
            }

//			this.lastPositionReport.clear();
//			this.lastPositionReport.add(vehiclePositions.GetAndUpdate(0));
            lastPositionReport = vehiclePositions.get(0);
            this.vehiclePosition.set(inputPositionReport);
            this.lastVehiclePosition.set(lastPositionReport);
            if (this.vehiclePosition.equals(this.lastVehiclePosition)) {
                vehiclePositions.add(0, inputPositionReport.copy());
                if (vehiclePositions.size() >= 4) {
                    LOG.trace("Car {} stopped at {} ({})", vid, this.vehiclePosition,
                            inputPositionReport.getMinuteNumber());
                    if (vehiclePositions.size() > 4) {
                        assert (vehiclePositions.size() == 5);
                        vehiclePositions.remove(4);
                    }
                    Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.vehiclePosition);
                    if (stoppedCars == null) {
                        stoppedCars = new HashSet<>();
                        stoppedCars.add(vid);
                        this.stoppedCarsPerPosition.put(this.vehiclePosition.copy(), stoppedCars);
                    } else {
                        stoppedCars.add(vid);
                        PositionReport inputPositionReport_cp = inputPositionReport.copy();
                        if (stoppedCars.size() > 1) {
//							cnt2++;
//							this.collector.emit(
//									ACCIDENTS_STREAM_ID2,
//									bid,
//									new AccidentTuple(inputPositionReport_cp, minute,
//											inputPositionReport_cp.getXWay(), inputPositionReport_cp.getSegment(),
//											inputPositionReport_cp.getDirection()));

                            LOG.trace("ACCIDENTS detection stream 2 (" + this.getContext().getThisTaskId() + ") emit bid:" + bid);
                        }
                    }

                }
            } else {
                if (vehiclePositions.size() == 4) {
                    Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                    stoppedCars.remove(vid);
                    if (stoppedCars.size() == 0) {
                        this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                    }
                }
                vehiclePositions.clear();
                vehiclePositions.add(inputPositionReport.copy());
            }
//			cnt++;

        }

    }

    public void display() {

        LOG.info("cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt) + "\tcnt2:" + cnt2 + "\toutput selectivity:" + ((cnt2) / cnt));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(ACCIDENTS_STREAM_ID, AccidentTuple.getSchema());
//		declarer.declareStream(ACCIDENTS_STREAM_ID2, AccidentTuple.getSchema());
    }

}
