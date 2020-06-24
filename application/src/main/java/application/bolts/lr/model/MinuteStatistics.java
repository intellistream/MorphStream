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

import java.util.HashMap;
import java.util.Map;


/**
 * Helper class that computes statistics associated with one segment, over one minute.
 *
 * @author richter
 */
class MinuteStatistics {

    private final Map<Integer, Integer> vehicleSpeeds = new HashMap<>();
    private double speedAverage; // rolling average for vehicles in this segment

    protected synchronized void addVehicleSpeed(int vehicleId, int vehicleSpeed) {
        double cumulativeSpeed = this.speedAverage * this.vehicleSpeeds.size();
        if (this.vehicleSpeeds.containsKey(vehicleId)) {
            int prevVehicleSpeed = this.vehicleSpeeds.get(vehicleId);
            cumulativeSpeed -= prevVehicleSpeed;
            cumulativeSpeed += (prevVehicleSpeed + vehicleSpeed) / 2.0;
        } else {
            this.vehicleSpeeds.put(vehicleId, vehicleSpeed);
            cumulativeSpeed += vehicleSpeed;
        }

        this.speedAverage = cumulativeSpeed / this.vehicleSpeeds.size();
    }

    protected synchronized double speedAverage() {
        return this.speedAverage;
    }

    protected synchronized int vehicleCount() {
        return this.vehicleSpeeds.size();
    }

    @Override
    public String toString() {
        return " [vehicleSpeeds=" + this.vehicleSpeeds + ", speedAverage=" + this.speedAverage + "]";
    }
}
