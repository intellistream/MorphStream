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
package common.datatype.util;
import org.apache.storm.tuple.Fields;
/**
 * PositionIdentifier represent an express way, lane, position, and direction.
 *
 * @author mjsax
 */
public final class PositionIdentifier implements IPositionIdentifier {
    private static final long serialVersionUID = -5932406731212707454L;
    /**
     * XWay (0...L−1) identifies the express way from which the position report is emitted.
     */
    private Integer xway;
    /**
     * Lane (0...4) identifies the lane of the expressway from which the position report is emitted.
     */
    private Short lane;
    /**
     * Pos (0...527999) identifies the horizontal position of the vehicle on the expressway.
     */
    private Integer position;
    /**
     * Dir (0,1) indicates the direction (0 for Eastbound and 1 for Westbound).
     */
    private Short direction;
    /**
     * Returns the schema of a {@link PositionIdentifier}.
     *
     * @return the schema of a {@link PositionIdentifier}
     */
    public static Fields getSchema() {
        return new Fields(LRTopologyControl.XWAY_FIELD_NAME, LRTopologyControl.LANE_FIELD_NAME,
                LRTopologyControl.POSITION_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME);
    }
    /**
     * Returns the express way ID.
     *
     * @return the express way ID
     */
    @Override
    public Integer getXWay() {
        return this.xway;
    }
    /**
     * Returns the lane ID.
     *
     * @return the lane ID
     */
    @Override
    public Short getLane() {
        return this.lane;
    }
    /**
     * Return the position.
     *
     * @return the position
     */
    @Override
    public Integer getPosition() {
        return this.position;
    }
    /**
     * Returns the direction.
     *
     * @return the direction
     */
    @Override
    public Short getDirection() {
        return this.direction;
    }
    /**
     * Set express way ID, lane, position, and direction from the given d_record.
     *
     * @param record The d_record this {@link PositionIdentifier} is populated from.
     */
    public PositionIdentifier set(IPositionIdentifier record) {
        assert (record != null);
        this.xway = record.getXWay();
        this.lane = record.getLane();
        this.position = record.getPosition();
        this.direction = record.getDirection();
        return this;
    }
    /**
     * Return a copy of this {@link PositionIdentifier}.
     *
     * @return a copy of this {@link PositionIdentifier}
     */
    public PositionIdentifier copy() {
        PositionIdentifier pid = new PositionIdentifier();
        pid.xway = this.xway;
        pid.lane = this.lane;
        pid.position = this.position;
        pid.direction = this.direction;
        return pid;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.direction == null) ? 0 : this.direction.hashCode());
        result = prime * result + ((this.lane == null) ? 0 : this.lane.hashCode());
        result = prime * result + ((this.position == null) ? 0 : this.position.hashCode());
        result = prime * result + ((this.xway == null) ? 0 : this.xway.hashCode());
        return result;
    }
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
        PositionIdentifier other = (PositionIdentifier) obj;
        if (this.direction == null) {
            if (other.direction != null) {
                return false;
            }
        } else if (!this.direction.equals(other.direction)) {
            return false;
        }
        if (this.lane == null) {
            if (other.lane != null) {
                return false;
            }
        } else if (!this.lane.equals(other.lane)) {
            return false;
        }
        if (this.position == null) {
            if (other.position != null) {
                return false;
            }
        } else if (!this.position.equals(other.position)) {
            return false;
        }
        if (this.xway == null) {
            return other.xway == null;
        } else {
            return this.xway.equals(other.xway);
        }
    }
    @Override
    public String toString() {
        return "x-way: " + this.xway + " lane: " + this.lane + " direction: " + this.direction + " position: "
                + this.position;
    }
}
