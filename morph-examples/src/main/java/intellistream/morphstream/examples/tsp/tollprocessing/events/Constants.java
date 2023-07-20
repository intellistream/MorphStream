/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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
package intellistream.morphstream.examples.tsp.tollprocessing.events;

/**
 * Constants related to the benchmark constraints. (See page 483 "Linear Road: A Streaming Data Management Benchmark")
 *
 * @author richter
 * @author mjsax
 */
public final class Constants {
    /* 0...10799 */
    public final static short MAX_TIME_SECONDS = 10799;
    public final static short NUMBER_OF_SECONDS = MAX_TIME_SECONDS + 1;
    /* 0...MAXINT */
    public final static int MAX_VID = Integer.MAX_VALUE;
    public final static long NUMBER_OF_VIDS = MAX_VID + 1L;
    /* 0...100 */
    public static final int MAX_SPEED = 100;
    public static final int NUMBER_OF_SPEEDS = MAX_SPEED + 1;
    /* lanes: 0..4 */
    public static final Short l0 = new Short((short) 0);
    public static final Short l1 = new Short((short) 1);
    public static final Short l2 = new Short((short) 2);
    public static final Short l3 = new Short((short) 3);
    public static final Short l4 = new Short((short) 4);
    public static final short ENTRANCE_LANE = l0.shortValue();
    public static final short EXIT_LANE = l4.shortValue();
    public static final Short EASTBOUND = new Short((short) 0);
    public static final Short WESTBOUND = new Short((short) 1);
    /* 0...100 */
    public static final short MAX_SEGMENT = 99;
    public static final short NUMBER_OF_SEGMENT = MAX_SEGMENT + 1;
    /* 0...527999 */
    public final static int MAX_POSITION = 527999;
    public final static int NUMBER_OF_POSITIONS = MAX_POSITION + 1;
    /* "If the LAV [...] is greater than or equal to 40 MPH, [...], no toll is assessed." */
    public static final int TOLL_LAV_THRESHOLD = 40;
    /* "[...], or if the number of vehicles [...] was 50 or less [...], no toll is assessed." */
    public static final int TOLL_NUM_CARS_THRESHOLD = 50;

    private Constants() {
    }
}
