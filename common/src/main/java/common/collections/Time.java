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
package common.collections;
/**
 * Helper class to compute the current minute of given millisecond.
 *
 * @author msoyka
 * @author mjsax
 */
public class Time {
    private Time() {
    }
    /**
     * Computes the 'minute number' if a time (in seconds).
     * <p/>
     * The 'minute number' m is computed as: {@code m = floor(timestamp / 60) + 1}
     *
     * @param timestamp the timestamp value in seconds
     * @return the 'minute number' if the given timestamp
     */
    public static short getMinute(long timestamp) {
        assert (timestamp >= 0);
        return (short) ((timestamp / 60) + 1);
    }
    public static short getMinute(short timestamp) {
        assert (timestamp >= 0);
        return (short) ((timestamp / 60) + 1);
    }
}
