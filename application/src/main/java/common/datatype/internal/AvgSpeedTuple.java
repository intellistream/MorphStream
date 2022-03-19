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

import common.collections.Time;
import common.datatype.util.ISegmentIdentifier;
import common.datatype.util.LRTopologyControl;
import common.util.datatypes.StreamValues;
import execution.runtime.tuple.impl.Fields;

/**
 * {@link AvgSpeedTuple} represents an intermediate result tuple; the average speed of all vehicle in a segment within a
 * 'minute number' time frame (see {@link Time#getMinute(long)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR, AVGS
 * <ul>
 * <li>MINUTE: the 'minute number' of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>AVGS: the average speed of the vehicle</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AvgSpeedTuple extends StreamValues implements ISegmentIdentifier {
    /**
     * The index of the MINUTE attribute.
     */
    private final static int MINUTE_IDX = 0;
    // attribute indexes
    /**
     * The index of the XWAY attribute.
     */
    private final static int XWAY_IDX = 1;
    /**
     * The index of the SEGMENT attribute.
     */
    private final static int SEG_IDX = 2;
    /**
     * The index of the DIR attribute.
     */
    private final static int DIR_IDX = 3;
    /**
     * The index of the AVGS attribute.
     */
    private final static int AVGS_IDX = 4;
    private static final long serialVersionUID = 2759896465050962310L;

    public AvgSpeedTuple() {
    }

    /**
     * Instantiates a new {@link AvgSpeedTuple} for the given attributes.
     *
     * @param minute   the 'minute number' of the speed average
     * @param xway     the expressway the vehicle is on
     * @param segment  the segment number the vehicle is in
     * @param diretion the vehicle's driving direction
     * @param avgSpeed the average speed of the vehicle
     */
    public AvgSpeedTuple(Short minute, Integer xway, Short segment, Short diretion, Double avgSpeed) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);
        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
    }

    /**
     * Returns the schema of a {@link AvgSpeedTuple}..
     *
     * @return the schema of a {@link AvgSpeedTuple}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.MINUTE_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.SEGMENT_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME,
                LRTopologyControl.AVERAGE_SPEED_FIELD_NAME);
    }

    /**
     * Returns the 'minute number' of this {@link AvgSpeedTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinuteNumber() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final int getSegment() {
        return (Integer) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the vehicle's average speed of this {@link AvgSpeedTuple}.
     *
     * @return the average speed of this tuple
     */
    public final Double getAvgSpeed() {
        return (Double) super.get(AVGS_IDX);
    }
}
