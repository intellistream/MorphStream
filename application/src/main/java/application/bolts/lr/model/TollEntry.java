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

import java.io.Serializable;


/**
 * A data container to be used in {@link } which is storable using the Java Persistence API.
 *
 * @author richter
 */
public class TollEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    /*
     * internal implementation notes: - integer complies with the range of expected data in the LRB specification
     */
    private Integer vehicleIdentifier;
    private int xWay;
    /*
     * internal implementation notes: - day is a reserved SQL-99 word and there not used
     */
    private int aDay;
    private int toll;

    /**
     * creates an empty non-functional {@code TollEntry} (used by Persistence API only)
     */
    protected TollEntry() {
    }

    /**
     * @param vehicleIdentifier
     * @param xWay
     * @param aDay
     * @param toll
     */
    public TollEntry(Integer vehicleIdentifier, int xWay, int aDay, int toll) {
        this.vehicleIdentifier = vehicleIdentifier;
        this.xWay = xWay;
        this.aDay = aDay;
        this.toll = toll;
    }

    /**
     * @return the vehicleIdentifier
     */
    public Integer getVehicleIdentifier() {
        return vehicleIdentifier;
    }

    /**
     * @param vehicleIdentifier the vehicleIdentifier to set
     */
    /*
     * internal implementation notes: - peristence id shouldn't to be writable
     */
    protected void setVehicleIdentifier(Integer vehicleIdentifier) {
        this.vehicleIdentifier = vehicleIdentifier;
    }

    /**
     * @return the xWay
     */
    public int getxWay() {
        return xWay;
    }

    /**
     * @param xWay the xWay to set
     */
    /*
     * internal implementation notes: - doesn't need to be writable (hide as much as possible)
     */
    protected void setxWay(int xWay) {
        this.xWay = xWay;
    }

    /**
     * @return the day
     */
    public int getADay() {
        return aDay;
    }

    /**
     * @param aDay the day to set
     */
    /*
     * internal implementation notes: - doesn't need to be writable (hide as much as possible)
     */
    protected void setADay(int aDay) {
        this.aDay = aDay;
    }

    /**
     * @return the toll
     */
    public int getToll() {
        return toll;
    }

    /**
     * @param toll the toll to set
     */
    public void setToll(int toll) {
        this.toll = toll;
    }

}
