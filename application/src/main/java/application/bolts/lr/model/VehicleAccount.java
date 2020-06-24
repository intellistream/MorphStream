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

import application.datatype.AccountBalanceRequest;
import application.datatype.PositionReport;

import java.io.Serializable;


/**
 * Object that keeps account information for a vehicle.
 */
public class VehicleAccount implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int day = 70;
    private PositionReport inputPositionReport;
    private Integer vehicleIdentifier = 0;
    private int tollToday = 0;
    private Long tollTime = 0L;
    private Integer xWay;

    public VehicleAccount() {
    }

    public VehicleAccount(Integer vehicleIdentifier, Integer xWay) {
        this.vehicleIdentifier = vehicleIdentifier;
        this.xWay = xWay;
        // TODO checken ob er an einem tag nur ein xway belegen kann, evtl auch rausnehmen
    }

    public VehicleAccount(int calculatedToll, final PositionReport pos) {
        // if(pos.getTimer() == null) {
        // throw new IllegalArgumentException("compatibility timer of the position timer is null");
        // }
        PositionReport lpos = pos.copy();
        try {
            this.vehicleIdentifier = lpos.getVid();
            this.xWay = lpos.getXWay();
            this.tollToday += calculatedToll;
            this.tollTime = Long.valueOf(lpos.getTime());
        } catch (Exception e) {
            System.out.println("VehicleAccount, pos error:" + pos.copy().toString() + e.getMessage());
        }
        //this.assessToll(calculatedToll, pos.getTime().getOffset);
    }

    public VehicleAccount(int calculatedToll, int vid, int xway, long time) {
        this.vehicleIdentifier = vid;
        this.xWay = xway;
        this.tollToday += calculatedToll;
        this.tollTime = time;
        //this.assessToll(calculatedToll, pos.getTime().getOffset);
    }

    public VehicleAccount(AccountBalanceRequest bal) {
        this.vehicleIdentifier = bal.getVid();
    }

    public int getTollToday() {
        return tollToday;
    }

    public void setTollToday(int tollToday) {
        this.tollToday = tollToday;
    }

    public void updateToll(int toll) {
        this.tollToday += toll;
    }

    // /**
    // * Adds newly assesed toll to the current account.
    // *
    // * @param calculatedToll
    // * amount
    // * @param time
    // * of assesment
    // */
    // public final void assessToll(int calculatedToll, Long time) {
    //
    // this.tollToday += calculatedToll;
    // this.tollTime = time;
    //
    // }

    public AccountBalance getAccBalanceNotification(AccountBalanceRequest accBalReq) {
        // TODO nach zweiter meinung fragen: Benchmarkspezifikation
        // widerspricht sich bei der Reihenfolge der Werte des Outputtuples.

        return new AccountBalance(accBalReq.getTime(), accBalReq.getQid(), this.tollToday,
                this.tollTime, accBalReq.getTime());
    }
}
