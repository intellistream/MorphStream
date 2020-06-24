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

package application.datatype.internal;

import application.datatype.util.ISegmentIdentifier;
import application.datatype.util.LRTopologyControl;
import application.util.Time;
import application.util.datatypes.StreamValues;
import sesame.execution.runtime.tuple.impl.Fields;


/**
 * {@link LavTuple} represents an intermediate result tuple; the "latest average velocity" (LAV) of a segment within the
 * last 5 minutes (ie, 'minute number'; see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR, LAV
 * <ul>
 * <li>MINUTE: the 'minute number' of the LAV value_list</li>
 * <li>XWAY: the expressway for the LAV value_list</li>
 * <li>SEGMENT: the segment number for the LAV value_list</li>
 * <li>DIR: the direction for the LAV value_list</li>
 * <li>LAV: the latest average velocity of the segment identified by XWAY, SEGMENT, DIR</li>
 * </ul>
 *
 * @author mjsax
 */
public final class LavTuple extends StreamValues implements ISegmentIdentifier {
    // attribute indexes
    /**
     * The index of the Minutes attribute.
     */
    public final static int MIN_IDX = 0;

    /**
     * The index of the XWAY attribute.
     */
    public final static int XWAY_IDX = 1;

    /**
     * The index of the SEGMENT attribute.
     */
    public final static int SEG_IDX = 2;

    /**
     * The index of the DIR attribute.
     */
    public final static int DIR_IDX = 3;

    /**
     * The index of the LAV attribute.
     */
    public final static int LAV_IDX = 4;

    public final static int TIME_IDX = 5;


    public LavTuple() {
    }

    /**
     * Instantiates a new {@link LavTuple} for the given attributes.
     *
     * @param minute    the 'minute number' (in LRB seconds) of the speed average
     * @param xway      the expressway the vehicle is on
     * @param segment   the segment number the vehicle is in
     * @param direction the vehicle's driving direction
     * @param lav
     * @param i
     */
    public LavTuple(Short minute, Integer xway, Short segment, Short direction, Double lav, short time) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (direction != null);
        assert (lav != null);

        super.add(MIN_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, direction);
        super.add(LAV_IDX, lav);
        super.add(TIME_IDX, time);

        assert (super.size() == 6);
    }

    public static Fields getLatencySchema() {
        return null;
    }


    /**
     * Returns the timestamp of this {@link LavTuple}.
     *
     * @return the timestamp of this tuple
     */
    public final Short getTime() {
        return (Short) super.get(MIN_IDX);
    }

    /**
     * Returns the 'minute number' of this {@link LavTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final short getMinuteNumber() {
        return Time.getMinute(this.getTime().shortValue());
    }

    /**
     * Returns the expressway ID of this {@link LavTuple}.
     *
     * @return the expressway of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link LavTuple}.
     *
     * @return the segment of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link LavTuple}.
     *
     * @return the direction of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the latest average velocity (LAV) of this {@link LavTuple}.
     *
     * @return the latest average velocity (LAV) of this tuple
     */
    public final Double getLav() {
        return (Double) super.get(LAV_IDX);
    }

    /**
     * Returns the schema of a {@link LavTuple}.
     *
     * @return the schema of a {@link LavTuple}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.MINUTE_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.SEGMENT_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME,
                LRTopologyControl.LAST_AVERAGE_SPEED_FIELD_NAME,
                LRTopologyControl.TIME_FIELD_NAME
        );
    }


}
