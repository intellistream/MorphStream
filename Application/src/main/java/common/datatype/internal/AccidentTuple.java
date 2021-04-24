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
import execution.runtime.tuple.impl.Fields;
/**
 * {@link AccidentTuple} represents an intermediate result tuple; and reports and accident that occurred in a specific
 * segment and minute (ie, 'minute number'; see {@link Time#getMinute(short)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR
 * <ul>
 * <li>MINUTE: the 'minute number' of the accident</li>
 * <li>XWAY: the expressway in which the accident happened</li>
 * <li>SEGMENT: in which the accident happened</li>
 * <li>DIR: the direction in which the accident happened</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AccidentTuple extends common.util.datatypes.StreamValues implements ISegmentIdentifier {
    // attribute indexes
    /**
     * The index of the TIME attribute.
     */
    public final static int TIME_IDX = 0;
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
    public AccidentTuple() {
    }
    /**
     * Instantiates a new {@link AccidentTuple} for the given attributes.
     *
     * @param time      the timestamp of the accident
     * @param xway      the expressway the vehicle is on
     * @param segment   the segment number the vehicle is in
     * @param direction the vehicle's driving direction
     */
    public AccidentTuple(Short time, Integer xway, Short segment, Short direction) {
        assert (time != null);
        assert (xway != null);
        assert (segment != null);
        assert (direction != null);
        super.add(TIME_IDX, time);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, direction);
        assert (super.size() == 4);
    }
    /**
     * Returns the schema of a {@link AccidentTuple}.
     *
     * @return the schema of a {@link AccidentTuple}
     */
    public static Fields getSchema() {
        return new Fields(
                LRTopologyControl.TIMESTAMP_FIELD_NAME,
                LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.SEGMENT_FIELD_NAME,
                LRTopologyControl.DIRECTION_FIELD_NAME);
    }
    /**
     * Returns the timestamp (in LRB seconds) of this {@link AccidentTuple}.
     *
     * @return the timestamp of this tuple
     */
    public final Short getTime() {
        return (Short) super.get(TIME_IDX);
    }
    /**
     * Returns the 'minute number' of this {@link AccidentTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final short getMinuteNumber() {
        return Time.getMinute(this.getTime().shortValue());
    }
    /**
     * Returns the expressway ID of this {@link AccidentTuple}.
     *
     * @return the expressway of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }
    /**
     * Returns the segment of this {@link AccidentTuple}.
     *
     * @return the segment of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }
    /**
     * Returns the vehicle's direction of this {@link AccidentTuple}.
     *
     * @return the direction of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }
}
