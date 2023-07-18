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
import engine.stream.execution.runtime.tuple.impl.Fields;

/**
 * A {@link DailyExpenditureRequest} from the LRB data generator.<br />
 * <br />
 * Daily expenditure requests do have the following attributes: TYPE=3, TIME, VID, XWay, QID, day
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>VID: the unique vehicle ID making the request</li>
 * <li>XWay: the ID of the expressway on which an expenditure overhead_total is desired (1...L-1)</li>
 * <li>QID: the unique request ID</li>
 * <li>Day: the day on which an expenditure overhead_total is desired (1 is yesterday, 69 is 10 weeks ago)</li>
 * </ul>
 *
 * @author mjsax
 * @author richtekp
 */
public class DailyExpenditureRequest extends AbstractInputTuple {
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
     * The index of the day attribute.
     */
    private final static int DAY_IDX = 5;
    private static final long serialVersionUID = 5710564296782458284L;

    public DailyExpenditureRequest() {
        super();
    }

    /**
     * Instantiates a new daily expenditure request for the given attributes.
     *
     * @param time the time at which the request was issued (in LRB seconds)
     * @param vid  the vehicle identifier
     * @param xway the express way of the request
     * @param qid  the query identifier
     * @param day  the day of the request
     */
    public DailyExpenditureRequest(Short time, Integer vid, Integer xway, Integer qid, Short day) {
        super(DAILY_EXPENDITURE_REQUEST, time, vid);
        assert (xway != null);
        assert (qid != null);
        assert (day != null);
        super.add(XWAY_IDX, xway);
        super.add(QID_IDX, qid);
        super.add(DAY_IDX, day);
        assert (super.size() == 6);
    }

    /**
     * Returns the schema of a {@link DailyExpenditureRequest}.
     *
     * @return the schema of a {@link DailyExpenditureRequest}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.TYPE_FIELD_NAME, LRTopologyControl.TIME_FIELD_NAME,
                LRTopologyControl.VEHICLE_ID_FIELD_NAME, LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.QUERY_ID_FIELD_NAME, LRTopologyControl.DAY_FIELD_NAME);
    }

    /**
     * Returns the expressway ID of this {@link DailyExpenditureRequest}.
     *
     * @return the VID of this request
     */
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the query ID of this {@link DailyExpenditureRequest}.
     *
     * @return the QID of this request
     */
    public final Integer getQid() {
        return (Integer) super.get(QID_IDX);
    }

    /**
     * Returns the day of this {@link DailyExpenditureRequest}.
     *
     * @return the day of this request
     */
    public final Short getDay() {
        return (Short) super.get(DAY_IDX);
    }
}
