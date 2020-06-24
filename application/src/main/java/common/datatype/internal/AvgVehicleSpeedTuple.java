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
package common.datatype.internal;
import common.datatype.util.ISegmentIdentifier;
import common.datatype.util.LRTopologyControl;
import common.collections.Time;
import sesame.execution.runtime.tuple.impl.Fields;

import static common.constants.BaseConstants.BaseField.MSG_ID;
import static common.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
/**
 * {@link AvgVehicleSpeedTuple} represents an intermediate result tuple; the average speed of an vehicle in a segment
 * within a 'minute number' time frame (see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: VID, MINUTE, XWAY, SEGMENT, DIR, AVGS
 * <ul>
 * <li>VID: the vehicle id</li>
 * <li>MINUTE: the 'minute number' of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>AVGS: the average speed of the vehicle</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AvgVehicleSpeedTuple extends common.util.datatypes.StreamValues implements ISegmentIdentifier {
    /**
     * The index of the TIME attribute.
     */
    public final static int TIME_IDX = 6;
    // attribute indexes
    /**
     * The index of the VID attribute.
     */
    private final static int VID_IDX = 0;
    /**
     * The index of the MINUTE attribute.
     */
    private final static int MINUTE_IDX = 1;
    /**
     * The index of the XWAY attribute.
     */
    private final static int XWAY_IDX = 2;
    /**
     * The index of the SEGMENT attribute.
     */
    private final static int SEG_IDX = 3;
    /**
     * The index of the DIR attribute.
     */
    private final static int DIR_IDX = 4;
    /**
     * The index of the AVGS attribute.
     */
    private final static int AVGS_IDX = 5;
    private static final long serialVersionUID = 9178312919140032032L;
    public AvgVehicleSpeedTuple() {
    }
    /**
     * Instantiates a new {@link AvgVehicleSpeedTuple} for the given attributes.
     *
     * @param vid      the vehicle id
     * @param minute   the 'minute number' of the speed average
     * @param xway     the expressway the vehicle is on
     * @param segment  the segment number the vehicle is in
     * @param diretion the vehicle's driving direction
     * @param avgSpeed the average speed of the vehicle
     * @param time
     */
    public AvgVehicleSpeedTuple(Integer vid, Short minute, Integer xway, Short segment, Short diretion, Double avgSpeed, Short time) {
        assert (vid != null);
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);
        super.add(VID_IDX, vid);
        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
        super.add(TIME_IDX, time);
    }
    public AvgVehicleSpeedTuple(Integer vid, Short minute, Integer xway, Short segment, Short diretion, Double avgSpeed, Long msgId, Long sysStamp) {
        assert (vid != null);
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);
        super.add(VID_IDX, vid);
        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
        super.add(msgId);//6
        super.add(sysStamp);//7
    }
    /**
     * Returns the schema of a {@link AvgVehicleSpeedTuple}.
     *
     * @return the schema of a {@link AvgVehicleSpeedTuple}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.VEHICLE_ID_FIELD_NAME, LRTopologyControl.MINUTE_FIELD_NAME,
                LRTopologyControl.XWAY_FIELD_NAME, LRTopologyControl.SEGMENT_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME,
                LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME);
    }
    public static Fields getLatencySchema() {
        return new Fields(LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME, MSG_ID, SYSTEMTIMESTAMP);
    }
    /**
     * Returns the vehicle ID of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    public final Integer getVid() {
        return (Integer) super.get(VID_IDX);
    }
    /**
     * Returns the 'minute number' of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinute() {
        return (Short) super.get(MINUTE_IDX);
    }
    /**
     * Returns the expressway ID of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }
    /**
     * Returns the segment of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }
    /**
     * Returns the vehicle's direction of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }
//
//    public long getMsgID() {
//        return (long) super.GetAndUpdate(6);
//    }
//
//    public long getTimeStamp() {
//        return (long) super.GetAndUpdate(7);
//    }
    /**
     * Returns the vehicle's average speed of this {@link AvgVehicleSpeedTuple}.
     *
     * @return the average speed of this tuple
     */
    public final Double getAvgSpeed() {
        return (Double) super.get(AVGS_IDX);
    }
    public final short getTime() {
        return (short) super.get(TIME_IDX);
    }
}
