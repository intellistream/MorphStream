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

import static common.constants.BaseConstants.BaseField.MSG_ID;
import static common.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;
/**
 * An {@link AccidentNotification} represent an alert that must be sent to vehicle approaching an accident.
 * <p/>
 * Accident notifications do have the following attributes: TYPE=1, TIME, EMIT, SEG, VID<br />
 * (the VID attribute is not part of the LRB specification; we need to add it to know the consumer of the notification)
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: the timestamp of the {@link PositionReport} that triggered the accident notification to be generated (in
 * LRB seconds)</li>
 * <li>EMIT: 'the time the notification is emitted' (in LRB seconds)</li>
 * <li>SEG: 'the segment where the accident occured'</li>
 * <li>VID: the vehicle that is notified about an accident, ie, the consumer of this message</li>
 * </ul>
 *
 * @author mjsax
 */
public class AccidentNotification extends AbstractOutputTuple {
    /**
     * The index of the segment attribute.
     */
    private final static int SEG_IDX = 3;
    // attribute indexes
    /**
     * The index of the VID attribute. (Not part of LRB specification but necessary for "mimic" message delivery to
     * correct car.)
     */
    private final static int VID_IDX = 4;
    private static final long serialVersionUID = -2731071679224249483L;
    public AccidentNotification() {
        super();
    }
    /**
     * Instantiates a new accident notification for the given attributes.
     *
     * @param time    the time or the position d_record triggering this notification
     * @param emit    the emit time of the notification
     * @param segment the accident segment
     * @param vid     the vehicle ID this notification has to be delivered to
     */
    public AccidentNotification(Short time, Short emit, Short segment, Integer vid) {
        super(ACCIDENT_NOTIFICATION, time, emit);
        assert (segment != null);
        super.add(SEG_IDX, segment);
        super.add(VID_IDX, vid);
        assert (super.size() == 5);
    }
    /**
     * Returns the schema of an {@link AccidentNotification}.
     *
     * @return the schema of an {@link AccidentNotification}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.TYPE_FIELD_NAME, LRTopologyControl.TIME_FIELD_NAME,
                LRTopologyControl.EMIT_FIELD_NAME, LRTopologyControl.SEGMENT_FIELD_NAME, LRTopologyControl.VEHICLE_ID_FIELD_NAME);
    }
    public static Fields getLatencySchema() {
        return new Fields(LRTopologyControl.ACCIDENT_NOTIFICATION_FIELD_NAME
                , MSG_ID, SYSTEMTIMESTAMP);
    }
    /**
     * Returns the segment of this {@link AccidentNotification}.
     *
     * @return the SEG of the accident
     */
    private Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }
    /**
     * Returns the vehicle ID of this {@link AccidentNotification}.
     *
     * @return the VID of this tuple
     */
    private Integer getVid() {
        return (Integer) super.get(VID_IDX);
    }
    /**
     * Compares the specified object with this {@link AccidentNotification} object for equality. Returns true if and
     * only if the specified object is also a {@link AccidentNotification} and their TIME, SEGMENT, and VID attributes
     * are equals. The EMIT attribute is not considered. Furthermore, TYPE is known to be equal if the specified object
     * is of type {@link AccidentNotification}.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        AccidentNotification other = (AccidentNotification) obj;
        assert (this.getType().equals(other.getType()));
        if (this.getTime() == null) {
            if (other.getTime() != null) {
                return false;
            }
        } else if (!this.getTime().equals(other.getTime())) {
            return false;
        }
        if (this.getSegment() == null) {
            if (other.getSegment() != null) {
                return false;
            }
        } else if (!this.getSegment().equals(other.getSegment())) {
            return false;
        }
        if (this.getVid() == null) {
            return other.getVid() == null;
        } else {
            return this.getVid().equals(other.getVid());
        }
    }
}
