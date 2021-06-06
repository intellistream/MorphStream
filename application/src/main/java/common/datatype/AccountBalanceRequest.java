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
 * A {@link AccountBalanceRequest} from the LRB data generator.<br />
 * <br />
 * Account balance requests do have the following attributes: TYPE=2, TIME, VID, QID
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>VID: the unique vehicle ID</li>
 * <li>QID: the unique request ID</li>
 * </ul>
 *
 * @author mjsax
 * @author richtekp
 */
public class AccountBalanceRequest extends AbstractInputTuple {
    /**
     * The index of the query identifier attribute.
     */
    private final static int QID_IDX = 3;
    // attribute indexes
    private static final long serialVersionUID = -7472179619183838842L;

    public AccountBalanceRequest(Short time, Integer vid, Integer qid, PositionReport objects) {
        super(ACCOUNT_BALANCE_REQUEST, time, vid);
        assert (qid != null);
        super.add(QID_IDX, qid);
        super.add(objects);
        assert (super.size() == 5);
    }

    /**
     * Instantiates a new account balance request for the given attributes.
     *
     * @param time the time at which the request was issued (in LRB seconds)
     * @param vid  the vehicle identifier
     * @param qid  the query identifier
     */
    public AccountBalanceRequest(Short time, Integer vid, Integer qid) {
        super(ACCOUNT_BALANCE_REQUEST, time, vid);
        assert (qid != null);
        super.add(QID_IDX, qid);
        assert (super.size() == 4);
    }

    /**
     * Returns the schema of a {@link AccountBalanceRequest}.
     *
     * @return the schema of a {@link AccountBalanceRequest}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.TYPE_FIELD_NAME, LRTopologyControl.TIME_FIELD_NAME,
                LRTopologyControl.VEHICLE_ID_FIELD_NAME, LRTopologyControl.QUERY_ID_FIELD_NAME
//                , LRTopologyControl.POS_REPORT_FIELD_NAME
        );
    }

    /**
     * Returns the query ID of this {@link AccountBalanceRequest}.
     *
     * @return the QID of this request
     */
    public final Integer getQid() {
        return (Integer) super.get(QID_IDX);
    }
}
