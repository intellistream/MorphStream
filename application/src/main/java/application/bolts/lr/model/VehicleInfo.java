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
package application.bolts.lr.model;


import application.datatype.PositionReport;
import application.datatype.util.SegmentIdentifier;
import org.apache.commons.collections.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * Object representing a vehicle
 */
public class VehicleInfo implements Serializable {
    // TODO evtl unterschiedliche vehicle objekte zusammen tun

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(VehicleInfo.class);
    /**
     * TODO Prepared toll history of vehicle keeps toll history as DAy,XWAY,Tolls
     */
    private final MultiKeyMap tollHistory = new MultiKeyMap();
    private Integer vid;
    private int calculatedToll = 0;
    private PositionReport posreport;
    /**
     * Time of positionreport which was last processed
     */
    private long lastSendToll = -1;

    private VehicleInfo(Integer vid) {
        this.vid = vid;
    }

    private VehicleInfo() {

    }

    private VehicleInfo(PositionReport pos) {

        this.posreport = pos;
        this.vid = pos.getVid();

    }

    /**
     * Update vehicle information with new position report
     *
     * @param pos position report
     */
    public void updateInfo(PositionReport pos) {
        this.posreport = pos;

    }

    public int getToll() {
        return this.calculatedToll;
    }

    private void setToll(int toll) {
        this.calculatedToll = toll;
    }

    private SegmentIdentifier getSegmentIdentifier() {
        return null; // new SegmentIdentifier(this.posreport);
    }

    private long getLastReportTime() {
        return this.lastSendToll;
    }

    private Integer getXway() {
        return this.posreport.getXWay();
    }

    /**
     * can be used as a placeholder for reacting to position reports which do not cause emission of toll notifications
     *
     * @return notification
     */
    // public String getEmptyNotification(String why) {
    //
    // String notification = why + "***" + this.posreport.getTime() + "," + this.posreport.getProcessingTime() + "###"
    // + this.posreport.show() + "###";
    // return notification;
    // }
    public String getTollNotification(double lav, int toll, int nov) {

        this.setToll(toll);

        // in case of compatibility replaying of tuples:
        // measure_end if we have duplicate processing of position report?
        if (this.posreport.getTime() == this.lastSendToll) {
            return "";// getEmptyNotification("duplicate");
        }
        String notification = "";
        // "0," + this.vid + "," + this.posreport.getTime() + ","
        // + this.posreport.getTimer().getOffset() + "," + (int)lav + "," + toll + "***" + this.posreport.getTime()
        // + "," + this.posreport.getProcessingTime() + "###" + this.posreport.show() + "###" + nov;

        // measure_end if time requirements are met if not stop computation
        // long diff;
        // if(this.posreport.getProcessingTimeSec() > 5) {
        // LOG.error("Time Requirement not met: " + this.posreport.getProcessingTimeSec() + " for " + this.posreport
        // + "\n" + notification);
        // if(LOG.isDebugEnabled()) {
        // throw new Error("Time Requirement not met:" + this.posreport + "\n" + notification);
        // }
        // }
        this.lastSendToll = this.posreport.getTime();
        return notification;
    }

    @Override
    public String toString() {
        int day = 70;
        return "VehicleInfo [vid=" + this.vid + ", calculatedToll=" + this.calculatedToll + ", xway=" + this.getXway()
                + ", day=" + day + ", time=" + this.getLastReportTime() + ", tollHistory=" + this.tollHistory
                + ", segDir=" + this.getSegmentIdentifier() + "]";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + (this.vid != null ? this.vid.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final VehicleInfo other = (VehicleInfo) obj;
        return !(this.vid != other.vid && (this.vid == null || !this.vid.equals(other.vid)));
    }

}
