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
 * Object that holds the latest average speed and number of vehicles of the given minute
 */
public class NovLav implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int nov;
    private final double lav;
    private final int minute;

    public NovLav(int cnt, double lav, int minute) {
        this.nov = cnt;
        this.lav = lav;
        this.minute = minute;
    }

    protected NovLav() {
        this.nov = 0;
        this.lav = 0.0;
        this.minute = 0;
    }

    public double getLav() {
        return this.lav;
    }

    public int getMinute() {
        return this.minute;
    }

    public int getNov() {
        return this.nov;
    }

    public boolean isEmpty() {
        return this.nov == 0;
    }

    @Override
    public String toString() {
        return "NovLav [nov=" + this.nov + ", lav=" + this.lav + ", min=" + this.minute + "]";
    }
}
