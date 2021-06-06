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
package common.datatype;

import common.datatype.util.LRTopologyControl;
import execution.runtime.tuple.impl.Fields;

/**
 * A {@link TravelTimeRequest} from the LRB data generator.<br />
 * <br />
 * Travel time requests do have the following attributes: TYPE=4, TIME, VID, XWay, QID, S_init, S_end, DOW, TOD
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>VID: the unique vehicle ID making the request</li>
 * <li>XWay: the ID of the expressway upon which the journey occurs (1...L-1)</li>
 * <li>QID: the unique request ID</li>
 * <li>S_init: the start segment of the journey (0...99)</li>
 * <li>S_end: the end segment of the journey (0...99)</li>
 * <li>DOW: day of the week when the journey would take place (1...7)</li>
 * <li>TOD: minute number in the day when the journey would take place (1...1440)</li>
 * </ul>
 *
 * @author mjsax
 * @author richtekp
 */
public class TravelTimeRequest extends AbstractInputTuple {
    /**
     * The index of the express way attribute.
     */
    private final static int XWAY_IDX = 3;
    // attribute indexes
    /**
     * The index of the query identifier attribute.
     */
    private final static int QID_IDX = 4;
    /**
     * The index of the start segment attribute.
     */
    private final static int S_INIT_IDX = 5;
    /**
     * The index of the end segment attribute.
     */
    private final static int S_END_IDX = 6;
    /**
     * The index of the day of the week attribute.
     */
    private final static int DOW_IDX = 7;
    /**
     * The index of the minute number attribute.
     */
    private final static int TOD_IDX = 8;
    private static final long serialVersionUID = -7993444129202648467L;

    public TravelTimeRequest() {
        super();
    }

    /**
     * Instantiates a new travel time request for the given attributes.
     *
     * @param time  the time at which the request was issued (in LRB seconds)
     * @param vid   the vehicle identifier
     * @param xway  the express way of the planed travel
     * @param qid   the query identifier
     * @param sInit the start segment of the planed travel
     * @param sEnd  the end segment of the planed travel
     * @param dow   the day of the week when the journey would take place
     * @param tod   the minute number of the day when the journey would take place
     */
    public TravelTimeRequest(Short time, Integer vid, Integer xway, Integer qid, Short sInit, Short sEnd, Short dow,
                             Short tod) {
        super(TRAVEL_TIME_REQUEST, time, vid);
        assert (xway != null);
        assert (qid != null);
        assert (sInit != null);
        assert (sEnd != null);
        assert (dow != null);
        assert (tod != null);
        super.add(XWAY_IDX, xway);
        super.add(QID_IDX, qid);
        super.add(S_INIT_IDX, sInit);
        super.add(S_END_IDX, sEnd);
        super.add(DOW_IDX, dow);
        super.add(TOD_IDX, tod);
        assert (super.size() == 9);
    }

    /**
     * Returns the schema of a {@link TravelTimeRequest}.
     *
     * @return the schema of a {@link TravelTimeRequest}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.TYPE_FIELD_NAME, LRTopologyControl.TIME_FIELD_NAME,
                LRTopologyControl.VEHICLE_ID_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.QUERY_ID_FIELD_NAME, LRTopologyControl.START_SEGMENT_FIELD_NAME,
                LRTopologyControl.END_SEGMENT_FIELD_NAME, LRTopologyControl.DAY_OF_WEEK_FIELD_NAME,
                LRTopologyControl.TIME_OF_DAY_FIELD_NAME);
    }

    /**
     * Returns the expressway ID of this {@link TravelTimeRequest}.
     *
     * @return the VID of this travel time request
     */
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the query ID of this {@link TravelTimeRequest}.
     *
     * @return the QID of this travel time request
     */
    public final Integer getQid() {
        return (Integer) super.get(QID_IDX);
    }

    /**
     * Returns the start segment of this {@link TravelTimeRequest}.
     *
     * @return the start segment of this travel time request
     */
    public final Short getSinit() {
        return (Short) super.get(S_INIT_IDX);
    }

    /**
     * Returns the end segment of this {@link TravelTimeRequest}.
     *
     * @return the end segment of this travel time request
     */
    public final Short getSend() {
        return (Short) super.get(S_END_IDX);
    }

    /**
     * Returns the day-of-week of this {@link TravelTimeRequest}.
     *
     * @return the DOW of this travel time request
     */
    public final Short getDow() {
        return (Short) super.get(DOW_IDX);
    }

    /**
     * Returns the time-of-day of this {@link TravelTimeRequest}.
     *
     * @return the TOD of this travel time request
     */
    public final Short getTod() {
        return (Short) super.get(TOD_IDX);
    }
}
